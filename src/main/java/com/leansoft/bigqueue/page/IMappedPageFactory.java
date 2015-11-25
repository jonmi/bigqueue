package com.leansoft.bigqueue.page;

import java.util.Set;

/**
 * Memory mapped page management ADT
 *
 * @author bulldog
 *
 */
public interface IMappedPageFactory {

    /**
     * Acquire a mapped page with specific index from the factory
     *
     * @param index the index of the page
     * @return a mapped page
     */
    IMappedPage acquirePage(long index);

    /**
     * Return the mapped page to the factory,
     * calling thread release the page to inform the factory that it has finished with the page,
     * so the factory get a chance to recycle the page to save memory.
     *
     * @param index the index of the page
     */
    void releasePage(long index);

    /**
     * Current set page size, when creating pages, the factory will
     * only create pages with this size.
     *
     * @return an integer number
     */
    int getPageSize();

    String getPageDir();

    /**
     * delete a mapped page with specific index in this factory,
     * this call will remove the page from the cache if it is cached and
     * delete back file.
     *
     * @param index the index of the page
     */
    void deletePage(long index);

    void deletePages(Set<Long> indexes);

    /**
     * delete all mapped pages currently available in this factory,
     * this call will remove all pages from the cache and delete all back files.
     */
    void deleteAllPages();

    /**
     * remove all cached pages from the cache and close resources associated with the cached pages.
     */
    void releaseCachedPages();

    /**
     * Get all indexes of pages with last modified timestamp before the specific timestamp.
     *
     * @param timestamp the timestamp to check
     * @return a set of indexes
     */
    Set<Long> getPageIndexSetBefore(long timestamp);

    /**
     * Delete all pages with last modified timestamp before the specific timestamp.
     *
     * @param timestamp the timestamp to check
     */
    void deletePagesBefore(long timestamp);

    /**
     * Delete all pages before the specific index
     *
     * @param pageIndex page file index to check
     */
    void deletePagesBeforePageIndex(long pageIndex);

    long getPageFileLastModifiedTime(long index);

    /**
     * Get index of a page file with last modified timestamp closest to specific timestamp.
     *
     * @param timestamp the timestamp to check
     * @return a page index
     */
    long getFirstPageIndexBefore(long timestamp);

    /**
     * For test, get a list of indexes of current existing back files.
     *
     * @return a set of indexes
     */
    Set<Long> getExistingBackFileIndexSet();

    /**
     * For test, get current cache size
     *
     * @return an integer number
     */
    int getCacheSize();

    /**
     * Persist any changes in cached mapped pages
     */
    void flush();

    /**
     *
     * A set of back page file names
     *
     * @return file name set
     */
    Set<String> getBackPageFileSet();

    /**
     * Total size of all page files
     *
     * @return total size
     */
    long getBackPageFileSize();

}
