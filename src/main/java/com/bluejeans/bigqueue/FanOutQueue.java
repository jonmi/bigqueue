package com.bluejeans.bigqueue;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A big, fast and persistent queue implementation supporting fan out semantics.
 *
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging and swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 * 6. FANOUT - support fan out semantics, multiple consumers can independently consume a single queue without intervention,
 *                     everyone has its own queue front index.
 * 7. CLIENT MANAGED INDEX - support access by index and the queue index is managed at client side.
 *
 * @author bulldog
 *
 */
public class FanOutQueue implements Closeable {

    /**
     * Constant represents earliest timestamp
     */
    public static final long EARLIEST = -1;
    /**
     * Constant represents latest timestamp
     */
    public static final long LATEST = -2;

    final BigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name prefix for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX = "front_index_";

    final ConcurrentMap<String, QueueFront> queueFrontMap = new ConcurrentHashMap<String, QueueFront>();

    /**
     * A big, fast and persistent queue implementation with fandout support.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize the back data file size per page in bytes, see minimum allowed {@link BigArray#MINIMUM_DATA_PAGE_SIZE}
     */
    public FanOutQueue(final String queueDir, final String queueName, final int pageSize) {
        innerArray = new BigArray(queueDir, queueName, pageSize);
    }

    /**
     * A big, fast and persistent queue implementation with fanout support,
     * use default back data page size, see {@link BigArray#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     */
    public FanOutQueue(final String queueDir, final String queueName) {
        this(queueDir, queueName, BigArray.DEFAULT_DATA_PAGE_SIZE);
    }

    QueueFront getQueueFront(final String fanoutId) {
        QueueFront qf = this.queueFrontMap.get(fanoutId);
        if (qf == null) { // not in cache, need to create one
            qf = new QueueFront(fanoutId);
            final QueueFront found = this.queueFrontMap.putIfAbsent(fanoutId, qf);
            if (found != null) {
                qf.indexPageFactory.releaseCachedPages();
                qf = found;
            }
        }

        return qf;
    }

    public boolean isEmpty(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            return qf.index.get() == innerArray.getHeadIndex();

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    /**
     * Determines whether the queue is empty
     *
     * @return true if empty, false otherwise
     */

    public boolean isEmpty() {
        return this.innerArray.isEmpty();
    }

    /**
     * Adds an item at the back of the queue
     *
     * @param data to be enqueued data
     * @return index where the item was appended
     */

    public long enqueue(final byte[] data) {
        return innerArray.append(data);
    }

    /**
     * Retrieves and removes the front of a fan out queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */

    public byte[] dequeue(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            try {
                qf.writeLock.lock();

                if (qf.index.get() == innerArray.arrayHeadIndex.get())
                    return null; // empty

                final byte[] data = innerArray.get(qf.index.get());
                qf.incrementIndex();

                return data;
            }
            catch (final IndexOutOfBoundsException ex) {
                ex.printStackTrace();
                qf.resetIndex(); // maybe the back array has been truncated to limit size

                final byte[] data = innerArray.get(qf.index.get());
                qf.incrementIndex();

                return data;

            }
            finally {
                qf.writeLock.unlock();
            }

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    /**
     * Peek the item at the front of a fanout queue, without removing it from the queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */

    public byte[] peek(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            if (qf.index.get() == innerArray.getHeadIndex())
                return null; // empty

            return innerArray.get(qf.index.get());

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    /**
     * Peek the length of the item at the front of a fan out queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */

    public int peekLength(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            if (qf.index.get() == innerArray.getHeadIndex())
                return -1; // empty
            return innerArray.getItemLength(qf.index.get());

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    /**
     * Peek the timestamp of the item at the front of a fan out queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */

    public long peekTimestamp(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            if (qf.index.get() == innerArray.getHeadIndex())
                return -1; // empty
            return innerArray.getTimestamp(qf.index.get());

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    /**
     * Retrieves data item at the specific index of the queue
     *
     * @param index data item index
     * @return data at index
     */

    public byte[] get(final long index) {
        return this.innerArray.get(index);
    }

    /**
     * Get length of data item at specific index of the queue
     *
     * @param index data item index
     * @return length of data item
     */

    public int getLength(final long index) {
        return this.innerArray.getItemLength(index);
    }

    /**
     * Get timestamp of data item at specific index of the queue, this is the timestamp when corresponding item was appended into the queue.
     *
     * @param index data item index
     * @return timestamp of data item
     */

    public long getTimestamp(final long index) {
        return this.innerArray.getTimestamp(index);
    }

    /**
     * Remove all data before specific timestamp, truncate back files and advance the queue front if necessary.
     *
     * @param timestamp a timestamp
     */
    public void removeBefore(final long timestamp) {
        try {
            this.innerArray.arrayWriteLock.lock();

            this.innerArray.removeBefore(timestamp);
            for (final QueueFront qf : this.queueFrontMap.values())
                try {
                    qf.writeLock.lock();
                    qf.validateAndAdjustIndex();
                }
                finally {
                    qf.writeLock.unlock();
                }
        }
        finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    /**
     * Remove all data before specific timestamp, truncate back files and advance the queue front if necessary.
     *
     * @param index a index
     */
    public void removeBeforeByIndex(final long index) {
        try {
            this.innerArray.arrayWriteLock.lock();

            this.innerArray.removeBeforeIndex(index);
            for (final QueueFront qf : this.queueFrontMap.values())
                try {
                    qf.writeLock.lock();
                    qf.validateAndAdjustIndex();
                }
                finally {
                    qf.writeLock.unlock();
                }
        }
        finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    /**
     * Limit the back file size of this queue, truncate back files and advance the queue front if necessary.
     *
     * Note, this is a best effort call, exact size limit can't be guaranteed
     *
     * @param sizeLimit size limit
     */
    public void limitBackFileSize(final long sizeLimit) {
        try {
            this.innerArray.arrayWriteLock.lock();

            this.innerArray.limitBackFileSize(sizeLimit);

            for (final QueueFront qf : this.queueFrontMap.values())
                try {
                    qf.writeLock.lock();
                    qf.validateAndAdjustIndex();
                }
                finally {
                    qf.writeLock.unlock();
                }

        }
        finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    /**
     * Current total size of the back files of this queue
     *
     * @return total back file size
     */

    public long getBackFileSize() {
        return this.innerArray.getBackFileSize();
    }

    /**
     * Find an index closest to the specific timestamp when the corresponding item was enqueued.
     * to find latest index, use {@link #LATEST} as timestamp.
     * to find earliest index, use {@link #EARLIEST} as timestamp.
     *
     * @param timestamp when the corresponding item was appended
     * @return an index
     */

    public long findClosestIndex(final long timestamp) {
        try {
            this.innerArray.arrayReadLock.lock();

            if (timestamp == LATEST)
                return this.innerArray.getHeadIndex();
            if (timestamp == EARLIEST)
                return this.innerArray.getTailIndex();
            return this.innerArray.findClosestIndex(timestamp);

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    public void resetQueueFrontIndex(final String fanoutId, final long index) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);

            try {
                qf.writeLock.lock();

                if (index != this.innerArray.getHeadIndex())
                    this.innerArray.validateIndex(index);

                qf.index.set(index);
                qf.persistIndex();

            }
            finally {
                qf.writeLock.unlock();
            }

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    public long size(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            final long qFront = qf.index.get();
            final long qRear = innerArray.getHeadIndex();
            if (qFront <= qRear)
                return qRear - qFront;
            else
                return Long.MAX_VALUE - qFront + 1 + qRear;

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    /**
     * Total number of items remaining in the queue.
     *
     * @return total number
     */

    public long size() {
        return this.innerArray.size();
    }

    /**
     * Force to persist current state of the queue,
     *
     * normally, you don't need to flush explicitly since:
     * 1.) FanOutQueue will automatically flush a cached page when it is replaced out,
     * 2.) FanOutQueue uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
     *
     * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
     */

    public void flush() {
        try {
            this.innerArray.arrayReadLock.lock();

            for (final QueueFront qf : this.queueFrontMap.values())
                try {
                    qf.writeLock.lock();
                    qf.indexPageFactory.flush();
                }
                finally {
                    qf.writeLock.unlock();
                }
            innerArray.flush();

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            this.innerArray.arrayWriteLock.lock();

            for (final QueueFront qf : this.queueFrontMap.values())
                qf.indexPageFactory.releaseCachedPages();

            innerArray.close();
        }
        finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    public void removeAll() {
        try {
            this.innerArray.arrayWriteLock.lock();

            for (final QueueFront qf : this.queueFrontMap.values())
                try {
                    qf.writeLock.lock();
                    qf.index.set(0L);
                    qf.persistIndex();
                }
                finally {
                    qf.writeLock.unlock();
                }
            innerArray.removeAll();

        }
        finally {
            this.innerArray.arrayWriteLock.unlock();
        }
    }

    // Queue front wrapper
    class QueueFront {

        // fanout index
        final String fanoutId;

        // front index of the fanout queue
        final AtomicLong index = new AtomicLong();

        // factory for queue front index page management(acquire, release, cache)
        final MappedPageFactory indexPageFactory;

        // lock for queue front write management
        final Lock writeLock = new ReentrantLock();

        QueueFront(final String fanoutId) {
            try {
                FolderNameValidator.validate(fanoutId);
            }
            catch (final IllegalArgumentException ex) {
                throw new IllegalArgumentException("invalid fanout identifier", ex);
            }
            this.fanoutId = fanoutId;
            // the ttl does not matter here since queue front index page is always cached
            this.indexPageFactory = new MappedPageFactory(QUEUE_FRONT_INDEX_PAGE_SIZE,
                    innerArray.arrayDirectory + QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX + fanoutId, 10 * 1000/*does not matter*/);

            final MappedPage indexPage = this.indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

            final ByteBuffer indexBuffer = indexPage.getLocal(0);
            index.set(indexBuffer.getLong());
            validateAndAdjustIndex();
        }

        void validateAndAdjustIndex() {
            if (index.get() != innerArray.arrayHeadIndex.get())
                try {
                    innerArray.validateIndex(index.get());
                }
                catch (final IndexOutOfBoundsException ex) { // maybe the back array has been truncated to limit size
                    resetIndex();
                }
        }

        // reset queue front index to the tail of array
        void resetIndex() {
            index.set(innerArray.arrayTailIndex.get());

            this.persistIndex();
        }

        void incrementIndex() {
            long nextIndex = index.get();
            if (nextIndex == Long.MAX_VALUE)
                nextIndex = 0L; // wrap
            else
                nextIndex++;
            index.set(nextIndex);

            this.persistIndex();
        }

        void persistIndex() {
            // persist index
            final MappedPage indexPage = this.indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            final ByteBuffer indexBuffer = indexPage.getLocal(0);
            indexBuffer.putLong(index.get());
            indexPage.setDirty(true);
        }
    }

    /**
     * Get the queue front index, this is the earliest appended index
     *
     * @return an index
     */

    public long getFrontIndex() {
        return this.innerArray.getTailIndex();
    }

    /**
     * Get the queue rear index, this is the next to be appended index
     *
     * @return an index
     */

    public long getRearIndex() {
        return this.innerArray.getHeadIndex();
    }

    public long getFrontIndex(final String fanoutId) {
        try {
            this.innerArray.arrayReadLock.lock();

            final QueueFront qf = this.getQueueFront(fanoutId);
            return qf.index.get();

        }
        finally {
            this.innerArray.arrayReadLock.unlock();
        }
    }
}
