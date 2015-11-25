package com.leansoft.bigqueue.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.Random;

import org.junit.Test;

import com.leansoft.bigqueue.TestUtil;

public class LRUCacheTest {

    @Test
    public void singleThreadTest() {

        final ILRUCache<Integer, TestObject> cache = new LRUCacheImpl<Integer, TestObject>();

        final TestObject obj = new TestObject();
        cache.put(1, obj, 500);

        TestObject obj1 = cache.get(1);
        assertEquals(obj, obj1);
        assertEquals(obj, obj1);
        assertFalse(obj1.isClosed());

        TestUtil.sleepQuietly(1000); // let 1 expire
        cache.put(2, new TestObject()); // trigger mark and sweep
        obj1 = cache.get(1);
        assertNotNull(obj1); // will not expire since there is reference count
        assertEquals(obj, obj1);
        assertFalse(obj1.isClosed());

        cache.release(1); // release first put
        cache.release(1); // release first get
        TestUtil.sleepQuietly(1000); // let 1 expire
        cache.put(3, new TestObject()); // trigger mark and sweep
        obj1 = cache.get(1);
        assertNotNull(obj1); // will not expire since there is reference count
        assertEquals(obj, obj1);
        assertFalse(obj1.isClosed());

        cache.release(1); // release second get
        cache.release(1); // release third get
        TestUtil.sleepQuietly(1000); // let 1 expire
        final TestObject testObj = new TestObject();
        cache.put(4, testObj); // trigger mark and sweep
        obj1 = cache.get(1);
        assertNull(obj1);

        TestUtil.sleepQuietly(1000); // let the cleaner do the job
        assertTrue(obj.isClosed());

        assertTrue(cache.size() == 3);

        final TestObject testObj2 = cache.remove(2);
        assertNotNull(testObj2);
        assertTrue(testObj2.closed);

        assertTrue(cache.size() == 2);

        final TestObject testObj3 = cache.remove(3);
        assertNotNull(testObj3);
        assertTrue(testObj3.closed);

        assertTrue(cache.size() == 1);

        cache.removeAll();

        TestUtil.sleepQuietly(1000); // let the cleaner do the job
        assertTrue(testObj.isClosed());

        assertTrue(cache.size() == 0);
    }

    @Test
    public void multiThreadsTest() {
        ILRUCache<Integer, TestObject> cache = new LRUCacheImpl<Integer, TestObject>();
        int threadNum = 100;

        final Worker[] workers = new Worker[threadNum];

        // initialization
        for (int i = 0; i < threadNum; i++)
            workers[i] = new Worker(i, cache);

        // run
        for (int i = 0; i < threadNum; i++)
            workers[i].start();

        // wait to finish
        for (int i = 0; i < threadNum; i++)
            try {
                workers[i].join();
            }
            catch (final InterruptedException e) {
                // ignore
            }

        assertTrue(cache.size() == 0);

        cache = new LRUCacheImpl<Integer, TestObject>();
        threadNum = 100;

        final RandomWorker[] randomWorkers = new RandomWorker[threadNum];
        final TestObject[] testObjs = new TestObject[threadNum];

        // initialization
        for (int i = 0; i < threadNum; i++) {
            testObjs[i] = new TestObject();
            cache.put(i, testObjs[i], 2000);
        }
        for (int i = 0; i < threadNum; i++)
            randomWorkers[i] = new RandomWorker(threadNum, cache);

        // run
        for (int i = 0; i < threadNum; i++)
            randomWorkers[i].start();

        // wait to finish
        for (int i = 0; i < threadNum; i++)
            try {
                randomWorkers[i].join();
            }
            catch (final InterruptedException e) {
                // ignore
            }

        // verification
        for (int i = 0; i < threadNum; i++) {
            final TestObject testObj = cache.get(i);
            assertNotNull(testObj);
            cache.release(i);
        }
        for (int i = 0; i < threadNum; i++)
            assertFalse(testObjs[i].isClosed());

        for (int i = 0; i < threadNum; i++)
            cache.release(i); // release put

        TestUtil.sleepQuietly(1000); // let the test objects expire but not expired
        cache.put(threadNum + 1, new TestObject()); // trigger mark and sweep

        for (int i = 0; i < threadNum; i++) {
            final TestObject testObj = cache.get(i);
            assertNotNull(testObj); // hasn't expire yet
            cache.release(i);
        }

        TestUtil.sleepQuietly(2010); // let the test objects expire and be expired
        final TestObject tmpObj = new TestObject();
        cache.put(threadNum + 1, tmpObj); // trigger mark and sweep

        for (int i = 0; i < threadNum; i++) {
            final TestObject testObj = cache.get(i);
            assertNull(testObj);
        }

        TestUtil.sleepQuietly(1000); // let the cleaner do the job
        for (int i = 0; i < threadNum; i++)
            assertTrue(testObjs[i].isClosed());

        assertTrue(cache.size() == 1);

        assertFalse(tmpObj.isClosed());

        cache.removeAll();
        TestUtil.sleepQuietly(1000);
        assertTrue(tmpObj.isClosed());

        assertTrue(cache.size() == 0);
    }

    private static class Worker extends Thread {
        private final int id;
        private final ILRUCache<Integer, TestObject> cache;

        public Worker(final int id, final ILRUCache<Integer, TestObject> cache) {
            this.id = id;
            this.cache = cache;
        }

        @Override
        public void run() {
            final TestObject testObj = new TestObject();
            cache.put(id, testObj, 500);

            final TestObject testObj2 = cache.get(id);
            assertEquals(testObj, testObj2);
            assertEquals(testObj, testObj2);
            assertFalse(testObj2.isClosed());

            cache.release(id); // release first put
            cache.release(id); // release first get

            TestUtil.sleepQuietly(1000);
            cache.put(id + 1000, new TestObject(), 500); // trigger mark&sweep

            final TestObject testObj3 = cache.get(id);
            assertNull(testObj3);
            TestUtil.sleepQuietly(1000); // let the cleaner do the job
            assertTrue(testObj.isClosed());

            cache.release(id + 1000);
            TestUtil.sleepQuietly(1000);
            cache.put(id + 2000, new TestObject()); // trigger mark&sweep

            final TestObject testObj_id2000 = cache.remove(id + 2000);
            assertNotNull(testObj_id2000);
            assertTrue(testObj_id2000.isClosed());

            final TestObject testObj4 = cache.get(id + 1000);
            TestUtil.sleepQuietly(1000); // let the cleaner do the job
            assertNull(testObj4);
        }
    }

    private static class RandomWorker extends Thread {
        private final int idLimit;
        private final Random random = new Random();
        private final ILRUCache<Integer, TestObject> cache;

        public RandomWorker(final int idLimit, final ILRUCache<Integer, TestObject> cache) {
            this.idLimit = idLimit;
            this.cache = cache;
        }

        @Override
        public void run() {
            for (int i = 0; i < 10; i++) {
                final int id = random.nextInt(idLimit);

                final TestObject testObj = cache.get(id);
                assertNotNull(testObj);
                cache.put(id + 1000, new TestObject(), 1000);
                cache.put(id + 2000, new TestObject(), 1000);
                cache.put(id + 3000, new TestObject(), 1000);
                cache.release(id + 1000);
                cache.release(id + 3000);
                cache.release(id);
                final TestObject testObj_id2000 = cache.remove(id + 2000);
                if (testObj_id2000 != null)
                    assertTrue(testObj_id2000.isClosed());
            }
        }
    }

    private static class TestObject implements Closeable {

        private volatile boolean closed = false;

        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

    }

}
