package se.ugli.bigqueue.page;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Test;

import se.ugli.bigqueue.TestUtil;
import se.ugli.bigqueue.utils.FileUtil;

public class MappedPageTest {

    private MappedPageFactory mappedPageFactory;
    private final String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit/mapped_page_test";

    @Test
    public void testSingleThread() {
        final int pageSize = 1024 * 1024 * 32;
        mappedPageFactory = new MappedPageFactory(pageSize, testDir + "/test_single_thread", 2 * 1000);

        final MappedPage mappedPage = this.mappedPageFactory.acquirePage(0);
        assertNotNull(mappedPage);

        ByteBuffer buffer = mappedPage.getLocal(0);
        assertTrue(buffer.limit() == pageSize);
        assertTrue(buffer.position() == 0);

        for (int i = 0; i < 10000; i++) {
            final String hello = "hello world";
            final int length = hello.getBytes().length;
            mappedPage.getLocal(i * 20).put(hello.getBytes());
            assertTrue(Arrays.equals(mappedPage.getLocal(i * 20, length), hello.getBytes()));
        }

        buffer = ByteBuffer.allocateDirect(16);
        buffer.putInt(1);
        buffer.putInt(2);
        buffer.putLong(3L);
        for (int i = 0; i < 10000; i++) {
            buffer.flip();
            mappedPage.getLocal(i * 20).put(buffer);
        }
        for (int i = 0; i < 10000; i++) {
            final ByteBuffer buf = mappedPage.getLocal(i * 20);
            assertTrue(1 == buf.getInt());
            assertTrue(2 == buf.getInt());
            assertTrue(3L == buf.getLong());
        }
    }

    @Test
    public void testMultiThreads() {
        final int pageSize = 1024 * 1024 * 32;
        mappedPageFactory = new MappedPageFactory(pageSize, testDir + "/test_multi_threads", 2 * 1000);

        final int threadNum = 100;
        final int pageNumLimit = 50;

        final Set<MappedPage> pageSet = Collections.newSetFromMap(new ConcurrentHashMap<MappedPage, Boolean>());
        final List<ByteBuffer> localBufferList = Collections.synchronizedList(new ArrayList<ByteBuffer>());

        final Worker[] workers = new Worker[threadNum];
        for (int i = 0; i < threadNum; i++)
            workers[i] = new Worker(i, mappedPageFactory, pageNumLimit, pageSet, localBufferList);
        for (int i = 0; i < threadNum; i++)
            workers[i].start();

        for (int i = 0; i < threadNum; i++)
            try {
                workers[i].join();
            }
            catch (final InterruptedException e) {
                // ignore
            }

        assertTrue(localBufferList.size() == threadNum * pageNumLimit);
        assertTrue(pageSet.size() == pageNumLimit);

        // verify thread locality
        for (int i = 0; i < localBufferList.size(); i++)
            for (int j = i + 1; j < localBufferList.size(); j++)
                if (localBufferList.get(i) == localBufferList.get(j))
                    fail("thread local buffer is not thread local");
    }

    private static class Worker extends Thread {
        private final int id;
        private final int pageNumLimit;
        private final MappedPageFactory pageFactory;
        private final Set<MappedPage> sharedPageSet;
        private final List<ByteBuffer> localBufferList;

        public Worker(final int id, final MappedPageFactory pageFactory, final int pageNumLimit,
                final Set<MappedPage> sharedPageSet, final List<ByteBuffer> localBufferList) {
            this.id = id;
            this.pageFactory = pageFactory;
            this.sharedPageSet = sharedPageSet;
            this.localBufferList = localBufferList;
            this.pageNumLimit = pageNumLimit;

        }

        @Override
        public void run() {
            for (int i = 0; i < pageNumLimit; i++) {
                final MappedPage page = this.pageFactory.acquirePage(i);
                sharedPageSet.add(page);
                localBufferList.add(page.getLocal(0));

                final int startPosition = this.id * 2048;

                for (int j = 0; j < 100; j++) {
                    final String helloj = "hello world " + j;
                    final int length = helloj.getBytes().length;
                    page.getLocal(startPosition + j * 20).put(helloj.getBytes());
                    assertTrue(Arrays.equals(page.getLocal(startPosition + j * 20, length), helloj.getBytes()));
                }

                final ByteBuffer buffer = ByteBuffer.allocateDirect(16);
                buffer.putInt(1);
                buffer.putInt(2);
                buffer.putLong(3L);
                for (int j = 0; j < 100; j++) {
                    buffer.flip();
                    page.getLocal(startPosition + j * 20).put(buffer);
                }
                for (int j = 0; j < 100; j++) {
                    final ByteBuffer buf = page.getLocal(startPosition + j * 20);
                    assertTrue(1 == buf.getInt());
                    assertTrue(2 == buf.getInt());
                    assertTrue(3L == buf.getLong());
                }

            }
        }

    }

    @After
    public void clear() {
        if (this.mappedPageFactory != null)
            this.mappedPageFactory.deleteAllPages();
        FileUtil.deleteDirectory(new File(testDir));
    }

}
