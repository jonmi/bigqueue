package se.ugli.bigqueue;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

public class MappedPageFactoryTest {

    private MappedPageFactory mappedPageFactory;
    private final String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit/mapped_page_factory_test";

    @Test
    public void testGetBackPageFileSet() {
        mappedPageFactory = new MappedPageFactory(1024, testDir + "/test_get_backpage_fileset", 2 * 1000);

        for (int i = 0; i < 10; i++)
            mappedPageFactory.acquirePage(i);

        final Set<String> fileSet = mappedPageFactory.getBackPageFileSet();
        assertTrue(fileSet.size() == 10);
        for (int i = 0; i < 10; i++)
            assertTrue(fileSet.contains(MappedPageFactory.PAGE_FILE_NAME + "-" + i + MappedPageFactory.PAGE_FILE_SUFFIX));
    }

    @Test
    public void testGetBackPageFileSize() {
        mappedPageFactory = new MappedPageFactory(1024 * 1024, testDir + "/test_get_backpage_filesize", 2 * 1000);

        for (int i = 0; i < 100; i++)
            mappedPageFactory.acquirePage(i);

        assertTrue(1024 * 1024 * 100 == mappedPageFactory.getBackPageFileSize());
    }

    @Test
    @Ignore
    public void testSingleThread() {

        mappedPageFactory = new MappedPageFactory(1024 * 1024 * 128, testDir + "/test_single_thread", 2 * 1000);

        MappedPage mappedPage = mappedPageFactory.acquirePage(0); // first acquire
        assertNotNull(mappedPage);
        final MappedPage mappedPage0 = mappedPageFactory.acquirePage(0); // second acquire
        assertSame(mappedPage, mappedPage0);

        final MappedPage mappedPage1 = mappedPageFactory.acquirePage(1);
        assertNotSame(mappedPage0, mappedPage1);

        mappedPageFactory.releasePage(0); // release first acquire
        mappedPageFactory.releasePage(0); // release second acquire
        TestUtil.sleepQuietly(2200);// let page0 expire
        mappedPageFactory.acquirePage(2);// trigger mark&sweep and purge old page0
        mappedPage = mappedPageFactory.acquirePage(0);// create a new page0
        assertNotSame(mappedPage, mappedPage0);
        TestUtil.sleepQuietly(1000);// let the async cleaner do the job
        assertTrue(!mappedPage.isClosed());
        assertTrue(mappedPage0.isClosed());

        for (long i = 0; i < 100; i++)
            assertNotNull(mappedPageFactory.acquirePage(i));
        assertTrue(mappedPageFactory.getCacheSize() == 100);
        Set<Long> indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertTrue(indexSet.size() == 100);
        for (long i = 0; i < 100; i++)
            assertTrue(indexSet.contains(i));

        this.mappedPageFactory.deletePage(0);
        assertTrue(mappedPageFactory.getCacheSize() == 99);
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertTrue(indexSet.size() == 99);

        this.mappedPageFactory.deletePage(1);
        assertTrue(mappedPageFactory.getCacheSize() == 98);
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertTrue(indexSet.size() == 98);

        for (long i = 2; i < 50; i++)
            this.mappedPageFactory.deletePage(i);
        assertTrue(mappedPageFactory.getCacheSize() == 50);
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertTrue(indexSet.size() == 50);

        this.mappedPageFactory.deleteAllPages();
        assertTrue(mappedPageFactory.getCacheSize() == 0);
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertTrue(indexSet.size() == 0);

        long start = System.currentTimeMillis();
        for (long i = 0; i < 5; i++) {
            assertNotNull(this.mappedPageFactory.acquirePage(i));
            TestUtil.sleepQuietly(1000);
        }
        indexSet = mappedPageFactory.getPageIndexSetBefore(start - 1000);
        assertTrue(indexSet.size() == 0);
        indexSet = mappedPageFactory.getPageIndexSetBefore(start + 2500);
        assertTrue(indexSet.size() == 3);
        indexSet = mappedPageFactory.getPageIndexSetBefore(start + 5000);
        assertTrue(indexSet.size() == 5);

        mappedPageFactory.deletePagesBefore(start + 2500);
        indexSet = mappedPageFactory.getExistingBackFileIndexSet();
        assertTrue(indexSet.size() == 2);
        assertTrue(mappedPageFactory.getCacheSize() == 2);

        mappedPageFactory.releaseCachedPages();
        assertTrue(mappedPageFactory.getCacheSize() == 0);

        assertTrue(mappedPageFactory.getLockMapSize() == 0);
        mappedPageFactory.deleteAllPages();

        start = System.currentTimeMillis();
        for (int i = 0; i <= 100; i++) {
            final MappedPage mappedPageI = mappedPageFactory.acquirePage(i);
            mappedPageI.getLocal(0).put(("hello " + i).getBytes());
            mappedPageI.setDirty(true);
            mappedPageI.flush();
            final long currentTime = System.currentTimeMillis();
            final long iPageFileLastModifiedTime = mappedPageFactory.getPageFileLastModifiedTime(i);
            assertTrue(iPageFileLastModifiedTime >= start);
            assertTrue(iPageFileLastModifiedTime <= currentTime);

            final long index = mappedPageFactory.getFirstPageIndexBefore(currentTime + 1);
            assertTrue(index == i);

            start = currentTime;
        }

        mappedPageFactory.deleteAllPages();

        // test wrapped case
        mappedPageFactory.acquirePage(Long.MAX_VALUE - 1);
        mappedPageFactory.acquirePage(Long.MAX_VALUE);
        long index = mappedPageFactory.getFirstPageIndexBefore(System.currentTimeMillis() + 1);
        assertTrue(index == Long.MAX_VALUE);

        mappedPageFactory.acquirePage(0);
        index = mappedPageFactory.getFirstPageIndexBefore(System.currentTimeMillis() + 1);
        assertTrue(index == 0);

        mappedPageFactory.acquirePage(1);
        index = mappedPageFactory.getFirstPageIndexBefore(System.currentTimeMillis() + 1);
        assertTrue(index == 1);
    }

    @After
    public void clear() {
        if (this.mappedPageFactory != null)
            this.mappedPageFactory.deleteAllPages();
        FileUtil.deleteDirectory(new File(testDir));
    }

    @Test
    public void testMultiThreads() {
        mappedPageFactory = new MappedPageFactory(1024 * 1024 * 128, testDir + "/test_multi_threads", 2 * 1000);

        final int pageNumLimit = 200;
        final int threadNum = 1000;

        final Map<Integer, MappedPage[]> sharedMap1 = this.testAndGetSharedMap(mappedPageFactory, threadNum, pageNumLimit);
        assertTrue(this.mappedPageFactory.getCacheSize() == pageNumLimit);
        final Map<Integer, MappedPage[]> sharedMap2 = this.testAndGetSharedMap(mappedPageFactory, threadNum, pageNumLimit);
        assertTrue(this.mappedPageFactory.getCacheSize() == pageNumLimit);
        // pages in two maps should be same since they are all cached
        verifyMap(sharedMap1, sharedMap2, threadNum, pageNumLimit, true);
        verifyClosed(sharedMap1, threadNum, pageNumLimit, false);

        TestUtil.sleepQuietly(2500);
        this.mappedPageFactory.acquirePage(pageNumLimit + 1); // trigger mark&sweep
        assertTrue(this.mappedPageFactory.getCacheSize() == 1);
        final Map<Integer, MappedPage[]> sharedMap3 = this.testAndGetSharedMap(mappedPageFactory, threadNum, pageNumLimit);
        assertTrue(this.mappedPageFactory.getCacheSize() == pageNumLimit + 1);
        // pages in two maps should be different since all pages in sharedMap1 has expired and purged out
        verifyMap(sharedMap1, sharedMap3, threadNum, pageNumLimit, false);
        verifyClosed(sharedMap3, threadNum, pageNumLimit, false);

        verifyClosed(sharedMap1, threadNum, pageNumLimit, true);

        // ensure no memory leak
        assertTrue(mappedPageFactory.getLockMapSize() == 0);
    }

    private void verifyClosed(final Map<Integer, MappedPage[]> map, final int threadNum, final int pageNumLimit, final boolean closed) {
        for (int i = 0; i < threadNum; i++) {
            final MappedPage[] pageArray = map.get(i);
            for (int j = 0; j < pageNumLimit; j++)
                if (closed)
                    assertTrue(pageArray[j].isClosed());
                else
                    assertTrue(!pageArray[j].isClosed());
        }
    }

    private void verifyMap(final Map<Integer, MappedPage[]> map1, final Map<Integer, MappedPage[]> map2, final int threadNum,
            final int pageNumLimit, final boolean same) {
        for (int i = 0; i < threadNum; i++) {
            final MappedPage[] pageArray1 = map1.get(i);
            final MappedPage[] pageArray2 = map2.get(i);
            for (int j = 0; j < pageNumLimit; j++)
                if (same)
                    assertSame(pageArray1[j], pageArray2[j]);
                else
                    assertNotSame(pageArray1[j], pageArray2[j]);
        }
    }

    private Map<Integer, MappedPage[]> testAndGetSharedMap(final MappedPageFactory pageFactory, final int threadNum,
            final int pageNumLimit) {

        // init shared map
        final Map<Integer, MappedPage[]> sharedMap = new ConcurrentHashMap<Integer, MappedPage[]>();
        for (int i = 0; i < threadNum; i++) {
            final MappedPage[] pageArray = new MappedPage[pageNumLimit];
            sharedMap.put(i, pageArray);
        }

        // init threads and start
        final CountDownLatch latch = new CountDownLatch(threadNum);
        final Worker[] workers = new Worker[threadNum];
        for (int i = 0; i < threadNum; i++) {
            workers[i] = new Worker(i, sharedMap, mappedPageFactory, pageNumLimit, latch);
            workers[i].start();
        }

        // wait to finish
        for (int i = 0; i < threadNum; i++)
            try {
                workers[i].join();
            }
            catch (final InterruptedException e) {
                // ignore silently
            }

        // validate
        final MappedPage[] firstPageArray = sharedMap.get(0);
        for (int j = 0; j < pageNumLimit; j++) {
            final MappedPage page = firstPageArray[j];
            assertTrue(!page.isClosed());
        }
        for (int i = 1; i < threadNum; i++) {
            final MappedPage[] pageArray = sharedMap.get(i);
            for (int j = 0; j < pageNumLimit; j++)
                assertSame(firstPageArray[j], pageArray[j]);
        }

        return sharedMap;
    }

    private static class Worker extends Thread {
        private final Map<Integer, MappedPage[]> map;
        private final MappedPageFactory pageFactory;
        private final int id;
        private final int pageNumLimit;
        private final CountDownLatch latch;

        public Worker(final int id, final Map<Integer, MappedPage[]> sharedMap, final MappedPageFactory mappedPageFactory,
                final int pageNumLimit, final CountDownLatch latch) {
            this.map = sharedMap;
            this.pageFactory = mappedPageFactory;
            this.id = id;
            this.pageNumLimit = pageNumLimit;
            this.latch = latch;
        }

        @Override
        public void run() {
            final List<Integer> pageNumList = new ArrayList<Integer>();
            for (int i = 0; i < pageNumLimit; i++)
                pageNumList.add(i);
            Collections.shuffle(pageNumList);
            final MappedPage[] pages = map.get(id);
            latch.countDown();
            try {
                latch.await();
            }
            catch (final InterruptedException e1) {
                // ignore silently
            }
            for (final int i : pageNumList) {
                pages[i] = this.pageFactory.acquirePage(i);
                this.pageFactory.releasePage(i);
            }
        }
    }
}
