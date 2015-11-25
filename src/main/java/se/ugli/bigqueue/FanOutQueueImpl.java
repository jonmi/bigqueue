package se.ugli.bigqueue;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import se.ugli.bigqueue.page.IMappedPage;
import se.ugli.bigqueue.page.IMappedPageFactory;
import se.ugli.bigqueue.page.MappedPageFactoryImpl;
import se.ugli.bigqueue.utils.FolderNameValidator;

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
public class FanOutQueueImpl implements IFanOutQueue {

    final BigArrayImpl innerArray;

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
     * @param pageSize the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     */
    public FanOutQueueImpl(final String queueDir, final String queueName, final int pageSize) {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);
    }

    /**
     * A big, fast and persistent queue implementation with fanout support,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     */
    public FanOutQueueImpl(final String queueDir, final String queueName) {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
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

    @Override
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

    @Override
    public boolean isEmpty() {
        return this.innerArray.isEmpty();
    }

    @Override
    public long enqueue(final byte[] data) {
        return innerArray.append(data);
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public byte[] get(final long index) {
        return this.innerArray.get(index);
    }

    @Override
    public int getLength(final long index) {
        return this.innerArray.getItemLength(index);
    }

    @Override
    public long getTimestamp(final long index) {
        return this.innerArray.getTimestamp(index);
    }

    @Override
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

    @Override
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

    @Override
    public long getBackFileSize() {
        return this.innerArray.getBackFileSize();
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    public long size() {
        return this.innerArray.size();
    }

    @Override
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

    @Override
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
        final IMappedPageFactory indexPageFactory;

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
            this.indexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                    innerArray.arrayDirectory + QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX + fanoutId, 10 * 1000/*does not matter*/);

            final IMappedPage indexPage = this.indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

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
            final IMappedPage indexPage = this.indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            final ByteBuffer indexBuffer = indexPage.getLocal(0);
            indexBuffer.putLong(index.get());
            indexPage.setDirty(true);
        }
    }

    @Override
    public long getFrontIndex() {
        return this.innerArray.getTailIndex();
    }

    @Override
    public long getRearIndex() {
        return this.innerArray.getHeadIndex();
    }

    @Override
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
