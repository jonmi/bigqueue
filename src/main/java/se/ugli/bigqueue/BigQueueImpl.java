package se.ugli.bigqueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import se.ugli.bigqueue.page.IMappedPage;
import se.ugli.bigqueue.page.IMappedPageFactory;
import se.ugli.bigqueue.page.MappedPageFactoryImpl;

/**
 * A big, fast and persistent queue implementation.
 *
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging and swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 *
 * @author bulldog
 */
public class BigQueueImpl implements IBigQueue {

    final IBigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();

    // lock for dequeueFuture access
    private final Object futureLock = new Object();
    private SettableFuture<byte[]> dequeueFuture;
    private SettableFuture<byte[]> peekFuture;

    /**
     * A big, fast and persistent queue implementation,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     */
    public BigQueueImpl(final String queueDir, final String queueName) {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     */
    public BigQueueImpl(final String queueDir, final String queueName, final int pageSize) {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl) innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER, 10 * 1000/*does not matter*/);
        final IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        final long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
    }

    @Override
    public boolean isEmpty() {
        return this.queueFrontIndex.get() == this.innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(final byte[] data) {
        this.innerArray.append(data);

        this.completeFutures();
    }

    @Override
    public byte[] dequeue() {
        long queueFrontIndex = -1L;
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty())
                return null;
            queueFrontIndex = this.queueFrontIndex.get();
            final byte[] data = this.innerArray.get(queueFrontIndex);
            long nextQueueFrontIndex = queueFrontIndex;
            if (nextQueueFrontIndex == Long.MAX_VALUE)
                nextQueueFrontIndex = 0L; // wrap
            else
                nextQueueFrontIndex++;
            this.queueFrontIndex.set(nextQueueFrontIndex);
            // persist the queue front
            final IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
            queueFrontIndexPage.setDirty(true);
            return data;
        }
        finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public ListenableFuture<byte[]> dequeueAsync() {
        this.initializeDequeueFutureIfNecessary();
        return dequeueFuture;
    }

    @Override
    public void removeAll() {
        try {
            queueFrontWriteLock.lock();
            this.innerArray.removeAll();
            this.queueFrontIndex.set(0L);
            final IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        }
        finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() {
        if (this.isEmpty())
            return null;
        final byte[] data = this.innerArray.get(this.queueFrontIndex.get());
        return data;
    }

    @Override
    public ListenableFuture<byte[]> peekAsync() {
        this.initializePeekFutureIfNecessary();
        return peekFuture;
    }

    @Override
    public void applyForEach(final ItemIterator iterator) {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty())
                return;

            final long index = this.queueFrontIndex.get();
            for (long i = index; i < this.innerArray.size(); i++)
                iterator.forEach(this.innerArray.get(i));
        }
        finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.queueFrontIndexPageFactory != null)
            this.queueFrontIndexPageFactory.releaseCachedPages();

        synchronized (futureLock) {
            /* Cancel the future but don't interrupt running tasks
            because they might perform further work not refering to the queue
             */
            if (peekFuture != null)
                peekFuture.cancel(false);
            if (dequeueFuture != null)
                dequeueFuture.cancel(false);
        }

        this.innerArray.close();
    }

    @Override
    public void gc() {
        long beforeIndex = this.queueFrontIndex.get();
        if (beforeIndex == 0L)
            beforeIndex = Long.MAX_VALUE;
        else
            beforeIndex--;
        try {
            this.innerArray.removeBeforeIndex(beforeIndex);
        }
        catch (final IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();
            this.queueFrontIndexPageFactory.flush();
            this.innerArray.flush();
        }
        finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public long size() {
        final long qFront = this.queueFrontIndex.get();
        final long qRear = this.innerArray.getHeadIndex();
        if (qFront <= qRear)
            return qRear - qFront;
        else
            return Long.MAX_VALUE - qFront + 1 + qRear;
    }

    /**
     * Completes the dequeue future
     */
    private void completeFutures() {
        synchronized (futureLock) {
            if (peekFuture != null && !peekFuture.isDone())
                peekFuture.set(this.peek());
            if (dequeueFuture != null && !dequeueFuture.isDone())
                dequeueFuture.set(this.dequeue());
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializeDequeueFutureIfNecessary() {
        synchronized (futureLock) {
            if (dequeueFuture == null || dequeueFuture.isDone())
                dequeueFuture = SettableFuture.create();
            if (!this.isEmpty())
                dequeueFuture.set(this.dequeue());
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializePeekFutureIfNecessary() {
        synchronized (futureLock) {
            if (peekFuture == null || peekFuture.isDone())
                peekFuture = SettableFuture.create();
            if (!this.isEmpty())
                peekFuture.set(this.peek());
        }
    }

}
