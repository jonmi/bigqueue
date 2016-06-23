package com.bluejeans.bigqueue;

import java.io.Closeable;
import java.io.File;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A big array implementation supporting sequential append and random read.
 *
 * Main features:
 * 1. FAST : close to the speed of direct memory access, extremely fast in append only and sequential read modes,
 *           sequential append and read are close to O(1) memory access, random read is close to O(1) memory access if
 *           data is in cache and is close to O(1) disk access if data is not in cache.
 * 2. MEMORY-EFFICIENT : automatic paging and swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently read/append the array without data corruption.
 * 4. PERSISTENT - all array data is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the array data is only limited by the available disk space.
 *
 *
 * @author bulldog
 *
 */
public class BigArray implements Closeable {

    public static final long NOT_FOUND = -1;

    // folder name for index page
    final static String INDEX_PAGE_FOLDER = "index";
    // folder name for data page
    final static String DATA_PAGE_FOLDER = "data";
    // folder name for meta data page
    final static String META_DATA_PAGE_FOLDER = "meta_data";

    // 2 ^ 20 = 1024 * 1024
    final static int INDEX_ITEMS_PER_PAGE_BITS = 20; // 1024 * 1024
    // number of items per page
    final static int INDEX_ITEMS_PER_PAGE = 1 << INDEX_ITEMS_PER_PAGE_BITS;
    // 2 ^ 5 = 32
    final static int INDEX_ITEM_LENGTH_BITS = 5;
    // length in bytes of an index item
    final static int INDEX_ITEM_LENGTH = 1 << INDEX_ITEM_LENGTH_BITS;
    // size in bytes of an index page
    final static int INDEX_PAGE_SIZE = INDEX_ITEM_LENGTH * INDEX_ITEMS_PER_PAGE;

    // size in bytes of a data page
    final int DATA_PAGE_SIZE;

    // default size in bytes of a data page
    public final static int DEFAULT_DATA_PAGE_SIZE = 128 * 1024 * 1024;
    // minimum size in bytes of a data page
    public final static int MINIMUM_DATA_PAGE_SIZE = 32 * 1024 * 1024;
    // seconds, time to live for index page cached in memory
    final static int INDEX_PAGE_CACHE_TTL = 1000;
    // seconds, time to live for data page cached in memory
    final static int DATA_PAGE_CACHE_TTL = 1000;
    // 2 ^ 4 = 16
    final static int META_DATA_ITEM_LENGTH_BITS = 4;
    // size in bytes of a meta data page
    final static int META_DATA_PAGE_SIZE = 1 << META_DATA_ITEM_LENGTH_BITS;

    //	private final static int INDEX_ITEM_DATA_PAGE_INDEX_OFFSET = 0;
    //	private final static int INDEX_ITEM_DATA_ITEM_OFFSET_OFFSET = 8;
    private final static int INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET = 12;
    // timestamp offset of an data item within an index item
    final static int INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET = 16;

    // directory to persist array data
    String arrayDirectory;

    // factory for index page management(acquire, release, cache)
    MappedPageFactory indexPageFactory;
    // factory for data page management(acquire, release, cache)
    MappedPageFactory dataPageFactory;
    // factory for meta data page management(acquire, release, cache)
    MappedPageFactory metaPageFactory;

    // only use the first page
    static final long META_DATA_PAGE_INDEX = 0;

    // head index of the big array, this is the read write barrier.
    // readers can only read items before this index, and writes can write this index or after
    final AtomicLong arrayHeadIndex = new AtomicLong();
    // tail index of the big array,
    // readers can't read items before this tail
    final AtomicLong arrayTailIndex = new AtomicLong();

    // head index of the data page, this is the to be appended data page index
    long headDataPageIndex;
    // head offset of the data page, this is the to be appended data offset
    int headDataItemOffset;

    // lock for appending state management
    final Lock appendLock = new ReentrantLock();

    // global lock for array read and write management
    final ReadWriteLock arrayReadWritelock = new ReentrantReadWriteLock();
    final Lock arrayReadLock = arrayReadWritelock.readLock();
    final Lock arrayWriteLock = arrayReadWritelock.writeLock();

    /**
     *
     * A big array implementation supporting sequential write and random read,
     * use default back data file size per page, see {@link #DEFAULT_DATA_PAGE_SIZE}.
     *
     * @param arrayDir directory for array data store
     * @param arrayName the name of the array, will be appended as last part of the array directory
     */
    public BigArray(final String arrayDir, final String arrayName) {
        this(arrayDir, arrayName, DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big array implementation supporting sequential write and random read.
     *
     * @param arrayDir directory for array data store
     * @param arrayName the name of the array, will be appended as last part of the array directory
     * @param pageSize the back data file size per page in bytes, see minimum allowed {@link #MINIMUM_DATA_PAGE_SIZE}.
     */
    public BigArray(final String arrayDir, final String arrayName, final int pageSize) {
        arrayDirectory = arrayDir;
        if (!arrayDirectory.endsWith(File.separator))
            arrayDirectory += File.separator;
        // append array name as part of the directory
        arrayDirectory = arrayDirectory + arrayName + File.separator;

        // validate directory
        if (!FileUtil.isFilenameValid(arrayDirectory))
            throw new IllegalArgumentException("invalid array directory : " + arrayDirectory);

        if (pageSize < MINIMUM_DATA_PAGE_SIZE)
            throw new IllegalArgumentException("invalid page size, allowed minimum is : " + MINIMUM_DATA_PAGE_SIZE + " bytes.");

        DATA_PAGE_SIZE = pageSize;

        this.commonInit();
    }

    String getArrayDirectory() {
        return this.arrayDirectory;
    }

    void commonInit() {
        // initialize page factories
        this.indexPageFactory = new MappedPageFactory(INDEX_PAGE_SIZE, this.arrayDirectory + INDEX_PAGE_FOLDER, INDEX_PAGE_CACHE_TTL);
        this.dataPageFactory = new MappedPageFactory(DATA_PAGE_SIZE, this.arrayDirectory + DATA_PAGE_FOLDER, DATA_PAGE_CACHE_TTL);
        // the ttl does not matter here since meta data page is always cached
        this.metaPageFactory = new MappedPageFactory(META_DATA_PAGE_SIZE, this.arrayDirectory + META_DATA_PAGE_FOLDER,
                10 * 1000/*does not matter*/);

        // initialize array indexes
        initArrayIndex();

        // initialize data page indexes
        initDataPageIndex();
    }

    public void removeAll() {
        try {
            arrayWriteLock.lock();
            this.indexPageFactory.deleteAllPages();
            this.dataPageFactory.deleteAllPages();
            this.metaPageFactory.deleteAllPages();
            //FileUtil.deleteDirectory(new File(this.arrayDirectory));

            this.commonInit();
        }
        finally {
            arrayWriteLock.unlock();
        }
    }

    /**
     * Remove all data before specific index, this will advance the array tail to index and
     * delete back page files before index.
     *
     * @param index an index
     */

    public void removeBeforeIndex(final long index) {
        try {
            arrayWriteLock.lock();

            validateIndex(index);

            final long indexPageIndex = Calculator.div(index, INDEX_ITEMS_PER_PAGE_BITS);

            final ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
            final long dataPageIndex = indexItemBuffer.getLong();

            if (indexPageIndex > 0L)
                this.indexPageFactory.deletePagesBeforePageIndex(indexPageIndex);
            if (dataPageIndex > 0L)
                this.dataPageFactory.deletePagesBeforePageIndex(dataPageIndex);

            // advance the tail to index
            this.arrayTailIndex.set(index);
        }
        finally {
            arrayWriteLock.unlock();
        }
    }

    /**
     * Remove all data before specific timestamp, this will advance the array tail and delete back page files
     * accordingly.
     *
     * @param timestamp a timestamp
     */

    public void removeBefore(final long timestamp) {
        try {
            arrayWriteLock.lock();
            final long firstIndexPageIndex = this.indexPageFactory.getFirstPageIndexBefore(timestamp);
            if (firstIndexPageIndex >= 0) {
                //				long nextIndexPageIndex = firstIndexPageIndex;
                //				if (nextIndexPageIndex == Long.MAX_VALUE) { //wrap
                //					nextIndexPageIndex = 0L;
                //				} else {
                //					nextIndexPageIndex++;
                //				}
                final long toRemoveBeforeIndex = Calculator.mul(firstIndexPageIndex, INDEX_ITEMS_PER_PAGE_BITS);
                removeBeforeIndex(toRemoveBeforeIndex);
            }
        }
        catch (final IndexOutOfBoundsException ex) {
            // ignore
        }
        finally {
            arrayWriteLock.unlock();
        }
    }

    // find out array head/tail from the meta data
    void initArrayIndex() {
        final MappedPage metaDataPage = this.metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
        final ByteBuffer metaBuf = metaDataPage.getLocal(0);
        final long head = metaBuf.getLong();
        final long tail = metaBuf.getLong();

        arrayHeadIndex.set(head);
        arrayTailIndex.set(tail);
    }

    // find out data page head index and offset
    void initDataPageIndex() {

        if (this.isEmpty()) {
            headDataPageIndex = 0L;
            headDataItemOffset = 0;
        }
        else {
            MappedPage previousIndexPage = null;
            long previousIndexPageIndex = -1;
            try {
                long previousIndex = this.arrayHeadIndex.get() - 1;
                if (previousIndex < 0)
                    previousIndex = Long.MAX_VALUE; // wrap
                previousIndexPageIndex = Calculator.div(previousIndex, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
                previousIndexPage = this.indexPageFactory.acquirePage(previousIndexPageIndex);
                final int previousIndexPageOffset = (int) Calculator.mul(Calculator.mod(previousIndex, INDEX_ITEMS_PER_PAGE_BITS),
                        INDEX_ITEM_LENGTH_BITS);
                final ByteBuffer previousIndexItemBuffer = previousIndexPage.getLocal(previousIndexPageOffset);
                final long previousDataPageIndex = previousIndexItemBuffer.getLong();
                final int previousDataItemOffset = previousIndexItemBuffer.getInt();
                final int perviousDataItemLength = previousIndexItemBuffer.getInt();

                headDataPageIndex = previousDataPageIndex;
                headDataItemOffset = previousDataItemOffset + perviousDataItemLength;
            }
            finally {
                if (previousIndexPage != null)
                    this.indexPageFactory.releasePage(previousIndexPageIndex);
            }
        }
    }

    /**
     * Append the data into the head of the array
     *
     * @param data binary data to append
     * @return appended index
     */

    public long append(final byte[] data) {
        try {
            arrayReadLock.lock();
            MappedPage toAppendDataPage = null;
            MappedPage toAppendIndexPage = null;
            long toAppendIndexPageIndex = -1L;
            long toAppendDataPageIndex = -1L;

            long toAppendArrayIndex = -1L;

            try {
                appendLock.lock(); // only one thread can append

                if (this.isFull())
                    throw new BigQueueException("ring space of java long type used up, the end of the world!!!");

                // prepare the data pointer
                if (this.headDataItemOffset + data.length > DATA_PAGE_SIZE) { // not enough space
                    if (this.headDataPageIndex == Long.MAX_VALUE)
                        this.headDataPageIndex = 0L; // wrap
                    else
                        this.headDataPageIndex++;
                    this.headDataItemOffset = 0;
                }

                toAppendDataPageIndex = this.headDataPageIndex;
                final int toAppendDataItemOffset = this.headDataItemOffset;

                toAppendArrayIndex = this.arrayHeadIndex.get();

                // append data
                toAppendDataPage = this.dataPageFactory.acquirePage(toAppendDataPageIndex);
                final ByteBuffer toAppendDataPageBuffer = toAppendDataPage.getLocal(toAppendDataItemOffset);
                toAppendDataPageBuffer.put(data);
                toAppendDataPage.setDirty(true);
                // update to next
                this.headDataItemOffset += data.length;

                toAppendIndexPageIndex = Calculator.div(toAppendArrayIndex, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
                toAppendIndexPage = this.indexPageFactory.acquirePage(toAppendIndexPageIndex);
                final int toAppendIndexItemOffset = (int) Calculator.mul(Calculator.mod(toAppendArrayIndex, INDEX_ITEMS_PER_PAGE_BITS),
                        INDEX_ITEM_LENGTH_BITS);

                // update index
                final ByteBuffer toAppendIndexPageBuffer = toAppendIndexPage.getLocal(toAppendIndexItemOffset);
                toAppendIndexPageBuffer.putLong(toAppendDataPageIndex);
                toAppendIndexPageBuffer.putInt(toAppendDataItemOffset);
                toAppendIndexPageBuffer.putInt(data.length);
                final long currentTime = System.currentTimeMillis();
                toAppendIndexPageBuffer.putLong(currentTime);
                toAppendIndexPage.setDirty(true);

                // advance the head
                this.arrayHeadIndex.incrementAndGet();

                // update meta data
                final MappedPage metaDataPage = this.metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
                final ByteBuffer metaDataBuf = metaDataPage.getLocal(0);
                metaDataBuf.putLong(this.arrayHeadIndex.get());
                metaDataBuf.putLong(this.arrayTailIndex.get());
                metaDataPage.setDirty(true);

            }
            finally {

                appendLock.unlock();

                if (toAppendDataPage != null)
                    this.dataPageFactory.releasePage(toAppendDataPageIndex);
                if (toAppendIndexPage != null)
                    this.indexPageFactory.releasePage(toAppendIndexPageIndex);
            }

            return toAppendArrayIndex;

        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * Force to persist newly appended data,
     *
     * normally, you don't need to flush explicitly since:
     * 1.) BigArray will automatically flush a cached page when it is replaced out,
     * 2.) BigArray uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
     *
     * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
     */

    public void flush() {
        try {
            arrayReadLock.lock();

            //			try {
            //				appendLock.lock(); // make flush and append mutually exclusive

            this.metaPageFactory.flush();
            this.indexPageFactory.flush();
            this.dataPageFactory.flush();

            //			} finally {
            //				appendLock.unlock();
            //			}

        }
        finally {
            arrayReadLock.unlock();
        }

    }

    /**
     * Get the data at specific index
     *
     * @param index valid data index
     * @return binary data if the index is valid
     */

    public byte[] get(final long index) {
        try {
            arrayReadLock.lock();
            validateIndex(index);

            MappedPage dataPage = null;
            long dataPageIndex = -1L;
            try {
                final ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
                dataPageIndex = indexItemBuffer.getLong();
                final int dataItemOffset = indexItemBuffer.getInt();
                final int dataItemLength = indexItemBuffer.getInt();
                dataPage = this.dataPageFactory.acquirePage(dataPageIndex);
                final byte[] data = dataPage.getLocal(dataItemOffset, dataItemLength);
                return data;
            }
            finally {
                if (dataPage != null)
                    this.dataPageFactory.releasePage(dataPageIndex);
            }
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * Get the timestamp of data at specific index,
     *
     * this is the timestamp when the data was appended.
     *
     * @param index valid data index
     * @return timestamp when the data was appended
     */

    public long getTimestamp(final long index) {
        try {
            arrayReadLock.lock();
            validateIndex(index);

            final ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
            // position to the timestamp
            final int position = indexItemBuffer.position();
            indexItemBuffer.position(position + INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET);
            final long ts = indexItemBuffer.getLong();
            return ts;
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    ByteBuffer getIndexItemBuffer(final long index) {

        MappedPage indexPage = null;
        long indexPageIndex = -1L;
        try {
            indexPageIndex = Calculator.div(index, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
            indexPage = this.indexPageFactory.acquirePage(indexPageIndex);
            final int indexItemOffset = (int) Calculator.mul(Calculator.mod(index, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS);

            final ByteBuffer indexItemBuffer = indexPage.getLocal(indexItemOffset);
            return indexItemBuffer;
        }
        finally {
            if (indexPage != null)
                this.indexPageFactory.releasePage(indexPageIndex);
        }
    }

    void validateIndex(final long index) {
        if (this.arrayTailIndex.get() <= this.arrayHeadIndex.get()) {
            if (index < this.arrayTailIndex.get() || index >= this.arrayHeadIndex.get())
                throw new IndexOutOfBoundsException();
        }
        else if (index < this.arrayTailIndex.get() && index >= this.arrayHeadIndex.get())
            throw new IndexOutOfBoundsException();
    }

    /**
     * The total number of items has been appended into the array
     *
     * @return total number
     */

    public long size() {
        try {
            arrayReadLock.lock();
            if (this.arrayTailIndex.get() <= this.arrayHeadIndex.get())
                return this.arrayHeadIndex.get() - this.arrayTailIndex.get();
            else
                return Long.MAX_VALUE - this.arrayTailIndex.get() + 1 + this.arrayHeadIndex.get();
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * The head of the array.
     *
     * This is the next to append index, the index of the last appended data
     * is [headIndex - 1] if the array is not empty.
     *
     * @return an index
     */

    public long getHeadIndex() {
        try {
            arrayReadLock.lock();
            return arrayHeadIndex.get();
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * The tail of the array.
     *
     * The is the index of the first appended data
     *
     * @return an index
     */

    public long getTailIndex() {
        try {
            arrayReadLock.lock();
            return arrayTailIndex.get();
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * Check if the array is empty or not
     *
     * @return true if empty false otherwise
     */

    public boolean isEmpty() {
        try {
            arrayReadLock.lock();
            return this.arrayHeadIndex.get() == this.arrayTailIndex.get();
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * Check if the ring space of java long type has all been used up.
     *
     * can always assume false, if true, the world is end:)
     *
     * @return array full or not
     */

    public boolean isFull() {
        try {
            arrayReadLock.lock();
            final long currentIndex = this.arrayHeadIndex.get();

            final long nextIndex = currentIndex == Long.MAX_VALUE ? 0 : currentIndex + 1;
            return nextIndex == this.arrayTailIndex.get();
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            arrayWriteLock.lock();
            if (this.metaPageFactory != null)
                this.metaPageFactory.releaseCachedPages();
            if (this.indexPageFactory != null)
                this.indexPageFactory.releaseCachedPages();
            if (this.dataPageFactory != null)
                this.dataPageFactory.releaseCachedPages();
        }
        finally {
            arrayWriteLock.unlock();
        }
    }

    /**
     * Get the back data file size per page.
     *
     * @return size per page
     */

    public int getDataPageSize() {
        return DATA_PAGE_SIZE;
    }

    /**
     * Find an index closest to the specific timestamp when the corresponding item was appended
     *
     * @param timestamp when the corresponding item was appended
     * @return an index
     */

    public long findClosestIndex(final long timestamp) {
        try {
            arrayReadLock.lock();
            long closestIndex = NOT_FOUND;
            final long tailIndex = this.arrayTailIndex.get();
            final long headIndex = this.arrayHeadIndex.get();
            if (tailIndex == headIndex)
                return closestIndex; // empty
            long lastIndex = headIndex - 1;
            if (lastIndex < 0)
                lastIndex = Long.MAX_VALUE;
            if (tailIndex < lastIndex)
                closestIndex = closestBinarySearch(tailIndex, lastIndex, timestamp);
            else {
                final long lowPartClosestIndex = closestBinarySearch(0L, lastIndex, timestamp);
                final long highPartClosetIndex = closestBinarySearch(tailIndex, Long.MAX_VALUE, timestamp);

                final long lowPartTimestamp = this.getTimestamp(lowPartClosestIndex);
                final long highPartTimestamp = this.getTimestamp(highPartClosetIndex);

                closestIndex = Math.abs(timestamp - lowPartTimestamp) < Math.abs(timestamp - highPartTimestamp) ? lowPartClosestIndex
                        : highPartClosetIndex;
            }

            return closestIndex;
        }
        finally {
            arrayReadLock.unlock();
        }
    }

    private long closestBinarySearch(final long low, final long high, final long timestamp) {
        long mid;
        final long sum = low + high;
        if (sum < 0) { // overflow
            BigInteger bigSum = BigInteger.valueOf(low);
            bigSum = bigSum.add(BigInteger.valueOf(high));
            mid = bigSum.shiftRight(1).longValue();
        }
        else
            mid = sum / 2;

        final long midTimestamp = this.getTimestamp(mid);

        if (midTimestamp < timestamp) {
            final long nextLow = mid + 1;
            if (nextLow >= high)
                return high;
            return closestBinarySearch(nextLow, high, timestamp);
        }
        else if (midTimestamp > timestamp) {
            final long nextHigh = mid - 1;
            if (nextHigh <= low)
                return low;
            return closestBinarySearch(low, nextHigh, timestamp);
        }
        else
            return mid;
    }

    /**
     * Get total size of back files(index and data files) of the big array
     *
     * @return total size of back files
     */

    public long getBackFileSize() {
        try {
            arrayReadLock.lock();

            return this._getBackFileSize();

        }
        finally {
            arrayReadLock.unlock();
        }
    }

    /**
     * limit the back file size, truncate back file and advance array tail index accordingly,
     * Note, this is a best effort call, exact size limit can't be guaranteed
     *
     * @param sizeLimit the size to limit
     */

    public void limitBackFileSize(final long sizeLimit) {
        if (sizeLimit < INDEX_PAGE_SIZE + DATA_PAGE_SIZE)
            return; // ignore, one index page + one data page are minimum for big array to work correctly

        long backFileSize = this.getBackFileSize();
        if (backFileSize <= sizeLimit)
            return; // nothing to do

        long toTruncateSize = backFileSize - sizeLimit;
        if (toTruncateSize < DATA_PAGE_SIZE)
            return; // can't do anything

        try {
            arrayWriteLock.lock();

            // double check
            backFileSize = this._getBackFileSize();
            if (backFileSize <= sizeLimit)
                return; // nothing to do

            toTruncateSize = backFileSize - sizeLimit;
            if (toTruncateSize < DATA_PAGE_SIZE)
                return; // can't do anything

            long tailIndex = this.arrayTailIndex.get();
            final long headIndex = this.arrayHeadIndex.get();
            long totalLength = 0L;
            while (true) {
                if (tailIndex == headIndex)
                    break;
                totalLength += this.getDataItemLength(tailIndex);
                if (totalLength > toTruncateSize)
                    break;

                if (tailIndex == Long.MAX_VALUE)
                    tailIndex = 0;
                else
                    tailIndex++;
                if (Calculator.mod(tailIndex, INDEX_ITEMS_PER_PAGE_BITS) == 0)
                    totalLength += INDEX_PAGE_SIZE;
            }
            this.removeBeforeIndex(tailIndex);
        }
        finally {
            arrayWriteLock.unlock();
        }

    }

    /**
     * Get the data item length at specific index
     *
     * @param index valid data index
     * @return the length of binary data if the index is valid
     */

    public int getItemLength(final long index) {
        try {
            arrayReadLock.lock();
            validateIndex(index);

            return getDataItemLength(index);

        }
        finally {
            arrayReadLock.unlock();
        }
    }

    private int getDataItemLength(final long index) {

        final ByteBuffer indexItemBuffer = this.getIndexItemBuffer(index);
        // position to the data item length
        final int position = indexItemBuffer.position();
        indexItemBuffer.position(position + INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET);
        final int length = indexItemBuffer.getInt();
        return length;
    }

    // inner getBackFileSize
    private long _getBackFileSize() {
        return this.indexPageFactory.getBackPageFileSize() + this.dataPageFactory.getBackPageFileSize();
    }
}
