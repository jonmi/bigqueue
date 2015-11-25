package se.ugli.bigqueue.page;

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

import org.apache.log4j.Logger;

import se.ugli.bigqueue.BigQueueException;
import se.ugli.bigqueue.cache.ILRUCache;
import se.ugli.bigqueue.cache.LRUCacheImpl;
import se.ugli.bigqueue.utils.CloseCommand;
import se.ugli.bigqueue.utils.FileUtil;

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
public class MappedPageFactoryImpl implements IMappedPageFactory {

    private final static Logger logger = Logger.getLogger(MappedPageFactoryImpl.class);

    private final int pageSize;
    private String pageDir;
    private final File pageDirFile;
    private final String pageFile;
    private final long ttl;

    private final Object mapLock = new Object();
    private final Map<Long, Object> pageCreationLockMap = new HashMap<Long, Object>();

    public static final String PAGE_FILE_NAME = "page";
    public static final String PAGE_FILE_SUFFIX = ".dat";

    private final ILRUCache<Long, MappedPageImpl> cache;

    public MappedPageFactoryImpl(final int pageSize, final String pageDir, final long cacheTTL) {
        this.pageSize = pageSize;
        this.pageDir = pageDir;
        this.ttl = cacheTTL;
        this.pageDirFile = new File(this.pageDir);
        if (!pageDirFile.exists())
            pageDirFile.mkdirs();
        if (!this.pageDir.endsWith(File.separator))
            this.pageDir += File.separator;
        this.pageFile = this.pageDir + PAGE_FILE_NAME + "-";
        this.cache = new LRUCacheImpl<Long, MappedPageImpl>();
    }

    @Override
    public IMappedPage acquirePage(final long index) {
        MappedPageImpl mpi = cache.get(index);
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
                            mpi = new MappedPageImpl(mbb, fileName, index);
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

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public String getPageDir() {
        return pageDir;
    }

    @Override
    public void releasePage(final long index) {
        cache.release(index);
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void releaseCachedPages() {
        cache.removeAll();
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
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
    @Override
    public void deletePages(final Set<Long> indexes) {
        if (indexes == null)
            return;
        for (final long index : indexes)
            this.deletePage(index);
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
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

    @Override
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

    @Override
    public void deletePagesBefore(final long timestamp) {
        final Set<Long> indexSet = this.getPageIndexSetBefore(timestamp);
        this.deletePages(indexSet);
        if (logger.isDebugEnabled())
            logger.debug("All page files in dir [" + this.pageDir + "], before [" + timestamp + "] have been deleted.");
    }

    @Override
    public void deletePagesBeforePageIndex(final long pageIndex) {
        final Set<Long> indexSet = this.getExistingBackFileIndexSet();
        for (final Long index : indexSet)
            if (index < pageIndex)
                this.deletePage(index);
    }

    @Override
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

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    // for testing
    int getLockMapSize() {
        return this.pageCreationLockMap.size();
    }

    @Override
    public long getPageFileLastModifiedTime(final long index) {
        final String pageFileName = this.getFileNameByIndex(index);
        final File pageFile = new File(pageFileName);
        if (!pageFile.exists())
            return -1L;
        return pageFile.lastModified();
    }

    @Override
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
     */
    @Override
    public void flush() {
        final Collection<MappedPageImpl> cachedPages = cache.getValues();
        for (final IMappedPage mappedPage : cachedPages)
            mappedPage.flush();
    }

    @Override
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

    @Override
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
