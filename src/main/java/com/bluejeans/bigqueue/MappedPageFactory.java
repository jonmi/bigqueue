package com.bluejeans.bigqueue;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapped mapped page resource manager,
 * responsible for the creation, cache, recycle of the mapped pages.
 *
 * automatic paging and swapping algorithm is leveraged to ensure fast page fetch while
 * keep memory usage efficient at the same time.
 *
 * @author bulldog
 *
 */
class MappedPageFactory {

    private final static Logger logger = LoggerFactory.getLogger(MappedPageFactory.class);

    private final int pageSize;
    private String pageDir;
    private final File pageDirFile;
    private final String pageFile;
    private final long ttl;

    private final Object mapLock = new Object();
    private final Map<Long, Object> pageCreationLockMap = new HashMap<Long, Object>();

    public static final String PAGE_FILE_NAME = "page";
    public static final String PAGE_FILE_SUFFIX = ".dat";

    private final LRUCache<Long, MappedPage> cache;

    public MappedPageFactory(final int pageSize, final String pageDir, final long cacheTTL) {
        this.pageSize = pageSize;
        this.pageDir = pageDir;
        this.ttl = cacheTTL;
        this.pageDirFile = new File(this.pageDir);
        if (!pageDirFile.exists())
            pageDirFile.mkdirs();
        if (!this.pageDir.endsWith(File.separator))
            this.pageDir += File.separator;
        this.pageFile = this.pageDir + PAGE_FILE_NAME + "-";
        this.cache = new LRUCache<Long, MappedPage>();
    }

    /**
     * Acquire a mapped page with specific index from the factory
     *
     * @param index the index of the page
     * @return a mapped page
     */

    public MappedPage acquirePage(final long index) {
        MappedPage mpi = cache.get(index);
        if (mpi == null)
            try {
                Object lock = null;
                synchronized (mapLock) {
                    if (!pageCreationLockMap.containsKey(index))
                        pageCreationLockMap.put(index, new Object());
                    lock = pageCreationLockMap.get(index);
                }
                synchronized (lock) { // only lock the creation of page index
                    mpi = cache.get(index); // double check
                    if (mpi == null) {
                        RandomAccessFile raf = null;
                        FileChannel channel = null;
                        try {
                            final String fileName = this.getFileNameByIndex(index);
                            raf = new RandomAccessFile(fileName, "rw");
                            channel = raf.getChannel();
                            final MappedByteBuffer mbb = channel.map(READ_WRITE, 0, this.pageSize);
                            mpi = new MappedPage(mbb, fileName, index);
                            cache.put(index, mpi, ttl);
                            if (logger.isDebugEnabled())
                                logger.debug("Mapped page for " + fileName + " was just created and cached.");
                        }
                        catch (final IOException e) {
                            throw new BigQueueException(e);
                        }
                        finally {
                            if (channel != null)
                                CloseCommand.close(channel);
                            if (raf != null)
                                CloseCommand.close(raf);
                        }
                    }
                }
            }
            finally {
                synchronized (mapLock) {
                    pageCreationLockMap.remove(index);
                }
            }
        else if (logger.isDebugEnabled())
            logger.debug("Hit mapped page " + mpi.getPageFile() + " in cache.");

        return mpi;
    }

    private String getFileNameByIndex(final long index) {
        return this.pageFile + index + PAGE_FILE_SUFFIX;
    }

    /**
     * Current set page size, when creating pages, the factory will
     * only create pages with this size.
     *
     * @return an integer number
     */

    public int getPageSize() {
        return pageSize;
    }

    public String getPageDir() {
        return pageDir;
    }

    /**
     * Return the mapped page to the factory,
     * calling thread release the page to inform the factory that it has finished with the page,
     * so the factory get a chance to recycle the page to save memory.
     *
     * @param index the index of the page
     */

    public void releasePage(final long index) {
        cache.release(index);
    }

    /**
     * thread unsafe, caller need synchronization
     *
     * remove all cached pages from the cache and close resources associated with the cached pages.
     */

    public void releaseCachedPages() {
        cache.removeAll();
    }

    /**
     * thread unsafe, caller need synchronization
     *
     * delete all mapped pages currently available in this factory,
     * this call will remove all pages from the cache and delete all back files.
     */

    public void deleteAllPages() {
        cache.removeAll();
        final Set<Long> indexSet = getExistingBackFileIndexSet();
        this.deletePages(indexSet);
        if (logger.isDebugEnabled())
            logger.debug("All page files in dir " + this.pageDir + " have been deleted.");
    }

    /**
     * thread unsafe, caller need synchronization
     */

    public void deletePages(final Set<Long> indexes) {
        if (indexes == null)
            return;
        for (final long index : indexes)
            this.deletePage(index);
    }

    /**
     * thread unsafe, caller need synchronization
     *
     * delete a mapped page with specific index in this factory,
     * this call will remove the page from the cache if it is cached and
     * delete back file.
     *
     * @param index the index of the page
     */

    public void deletePage(final long index) {
        // remove the page from cache first
        cache.remove(index);
        final String fileName = this.getFileNameByIndex(index);
        int count = 0;
        final int maxRound = 10;
        boolean deleted = false;
        while (count < maxRound)
            try {
                FileUtil.deleteFile(new File(fileName));
                deleted = true;
                break;
            }
            catch (final IllegalStateException ex) {
                try {
                    Thread.sleep(200);
                }
                catch (final InterruptedException e) {
                }
                count++;
                if (logger.isDebugEnabled())
                    logger.warn("fail to delete file " + fileName + ", tried round = " + count);
            }
        if (deleted)
            logger.info("Page file " + fileName + " was just deleted.");
        else
            logger.warn("fail to delete file " + fileName + " after max " + maxRound + " rounds of try, you may delete it manually.");
    }

    /**
     * Get all indexes of pages with last modified timestamp before the specific timestamp.
     *
     * @param timestamp the timestamp to check
     * @return a set of indexes
     */

    public Set<Long> getPageIndexSetBefore(final long timestamp) {
        final Set<Long> beforeIndexSet = new HashSet<Long>();
        final File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null && pageFiles.length > 0)
            for (final File pageFile : pageFiles)
                if (pageFile.lastModified() < timestamp) {
                    final String fileName = pageFile.getName();
                    if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                        final long index = this.getIndexByFileName(fileName);
                        beforeIndexSet.add(index);
                    }
                }
        return beforeIndexSet;
    }

    private long getIndexByFileName(final String fileName) {
        int beginIndex = fileName.lastIndexOf('-');
        beginIndex += 1;
        final int endIndex = fileName.lastIndexOf(PAGE_FILE_SUFFIX);
        final String sIndex = fileName.substring(beginIndex, endIndex);
        final long index = Long.parseLong(sIndex);
        return index;
    }

    /**
     * Delete all pages with last modified timestamp before the specific timestamp.
     *
     * @param timestamp the timestamp to check
     */

    public void deletePagesBefore(final long timestamp) {
        final Set<Long> indexSet = this.getPageIndexSetBefore(timestamp);
        this.deletePages(indexSet);
        if (logger.isDebugEnabled())
            logger.debug("All page files in dir [" + this.pageDir + "], before [" + timestamp + "] have been deleted.");
    }

    /**
     * Delete all pages before the specific index
     *
     * @param pageIndex page file index to check
     */

    public void deletePagesBeforePageIndex(final long pageIndex) {
        final Set<Long> indexSet = this.getExistingBackFileIndexSet();
        for (final Long index : indexSet)
            if (index < pageIndex)
                this.deletePage(index);
    }

    /**
     * For test, get a list of indexes of current existing back files.
     *
     * @return a set of indexes
     */

    public Set<Long> getExistingBackFileIndexSet() {
        final Set<Long> indexSet = new HashSet<Long>();
        final File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null && pageFiles.length > 0)
            for (final File pageFile : pageFiles) {
                final String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    final long index = this.getIndexByFileName(fileName);
                    indexSet.add(index);
                }
            }
        return indexSet;
    }

    /**
     * For test, get current cache size
     *
     * @return an integer number
     */

    public int getCacheSize() {
        return cache.size();
    }

    // for testing
    int getLockMapSize() {
        return this.pageCreationLockMap.size();
    }

    public long getPageFileLastModifiedTime(final long index) {
        final String pageFileName = this.getFileNameByIndex(index);
        final File pageFile = new File(pageFileName);
        if (!pageFile.exists())
            return -1L;
        return pageFile.lastModified();
    }

    /**
     * Get index of a page file with last modified timestamp closest to specific timestamp.
     *
     * @param timestamp the timestamp to check
     * @return a page index
     */

    public long getFirstPageIndexBefore(final long timestamp) {
        final Set<Long> beforeIndexSet = getPageIndexSetBefore(timestamp);
        if (beforeIndexSet.size() == 0)
            return -1L;
        final TreeSet<Long> sortedIndexSet = new TreeSet<Long>(beforeIndexSet);
        final Long largestIndex = sortedIndexSet.last();
        if (largestIndex != Long.MAX_VALUE)
            return largestIndex;
        else { // wrapped case
            Long next = 0L;
            while (sortedIndexSet.contains(next))
                next++;
            if (next == 0L)
                return Long.MAX_VALUE;
            else
                return --next;
        }
    }

    /**
     * thread unsafe, caller need synchronization
     *
     * Persist any changes in cached mapped pages
     */

    public void flush() {
        final Collection<MappedPage> cachedPages = cache.getValues();
        for (final MappedPage mappedPage : cachedPages)
            mappedPage.flush();
    }

    /**
    *
    * A set of back page file names
    *
    * @return file name set
    */

    public Set<String> getBackPageFileSet() {
        final Set<String> fileSet = new HashSet<String>();
        final File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null && pageFiles.length > 0)
            for (final File pageFile : pageFiles) {
                final String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX))
                    fileSet.add(fileName);
            }
        return fileSet;
    }

    /**
    *
    * A set of back page file names
    *
    * @return file name set
    */

    public long getBackPageFileSize() {
        long totalSize = 0L;
        final File[] pageFiles = this.pageDirFile.listFiles();
        if (pageFiles != null && pageFiles.length > 0)
            for (final File pageFile : pageFiles) {
                final String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX))
                    totalSize += pageFile.length();
            }
        return totalSize;
    }

}
