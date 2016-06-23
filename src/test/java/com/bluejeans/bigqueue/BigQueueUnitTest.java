package com.bluejeans.bigqueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.bluejeans.bigqueue.BigArray;
import com.bluejeans.bigqueue.BigQueue;
import com.bluejeans.bigqueue.ItemIterator;
import com.google.common.util.concurrent.ListenableFuture;

public class BigQueueUnitTest {

    private final String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit";
    private BigQueue bigQueue;

    @Test
    public void simpleTest() throws IOException {
        for (int i = 1; i <= 2; i++) {

            bigQueue = new BigQueue(testDir, "simple_test");
            assertNotNull(bigQueue);

            for (int j = 1; j <= 3; j++) {
                assertTrue(bigQueue.size() == 0L);
                assertTrue(bigQueue.isEmpty());

                assertNull(bigQueue.dequeue());
                assertNull(bigQueue.peek());

                bigQueue.enqueue("hello".getBytes());
                assertTrue(bigQueue.size() == 1L);
                assertTrue(!bigQueue.isEmpty());
                assertEquals("hello", new String(bigQueue.peek()));
                assertEquals("hello", new String(bigQueue.dequeue()));
                assertNull(bigQueue.dequeue());

                bigQueue.enqueue("world".getBytes());
                bigQueue.flush();
                assertTrue(bigQueue.size() == 1L);
                assertTrue(!bigQueue.isEmpty());
                assertEquals("world", new String(bigQueue.dequeue()));
                assertNull(bigQueue.dequeue());

            }

            bigQueue.close();

        }
    }

    @Test
    public void bigLoopTest() throws IOException {
        bigQueue = new BigQueue(testDir, "big_loop_test");
        assertNotNull(bigQueue);

        final int loop = 1000000;
        for (int i = 0; i < loop; i++) {
            bigQueue.enqueue(("" + i).getBytes());
            assertTrue(bigQueue.size() == i + 1L);
            assertTrue(!bigQueue.isEmpty());
            final byte[] data = bigQueue.peek();
            assertEquals("0", new String(data));
        }

        assertTrue(bigQueue.size() == loop);
        assertTrue(!bigQueue.isEmpty());
        assertEquals("0", new String(bigQueue.peek()));

        bigQueue.close();

        // create a new instance on exiting queue
        bigQueue = new BigQueue(testDir, "big_loop_test");
        assertTrue(bigQueue.size() == loop);
        assertTrue(!bigQueue.isEmpty());

        for (int i = 0; i < loop; i++) {
            final byte[] data = bigQueue.dequeue();
            assertEquals("" + i, new String(data));
            assertTrue(bigQueue.size() == loop - i - 1);
        }

        assertTrue(bigQueue.isEmpty());

        bigQueue.gc();

        bigQueue.close();
    }

    @Test
    public void loopTimingTest() {
        bigQueue = new BigQueue(testDir, "loop_timing_test");
        assertNotNull(bigQueue);

        final int loop = 1000000;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            bigQueue.enqueue(("" + i).getBytes());
        long end = System.currentTimeMillis();
        int timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to enqueue " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            assertEquals("" + i, new String(bigQueue.dequeue()));
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
    }

    @Test
    public void testInvalidDataPageSize() {
        try {
            bigQueue = new BigQueue(testDir, "testInvalidDataPageSize", BigArray.MINIMUM_DATA_PAGE_SIZE - 1);
            fail("should throw invalid page size exception");
        }
        catch (final IllegalArgumentException iae) {
            // ecpected
        }
        // ok
        bigQueue = new BigQueue(testDir, "testInvalidDataPageSize", BigArray.MINIMUM_DATA_PAGE_SIZE);
    }

    @Test
    public void testApplyForEachDoNotChangeTheQueue() throws Exception {
        bigQueue = new BigQueue(testDir, "testApplyForEachDoNotChangeTheQueue", BigArray.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue("1".getBytes());
        bigQueue.enqueue("2".getBytes());
        bigQueue.enqueue("3".getBytes());

        final DefaultItemIterator dii = new DefaultItemIterator();
        bigQueue.applyForEach(dii);
        System.out.println("[" + dii.getCount() + "] " + dii.toString());

        assertEquals(3, bigQueue.size());
        assertEquals(bigQueue.size(), dii.getCount());

        assertArrayEquals("1".getBytes(), bigQueue.dequeue());
        assertArrayEquals("2".getBytes(), bigQueue.dequeue());
        assertArrayEquals("3".getBytes(), bigQueue.dequeue());

        assertEquals(0, bigQueue.size());
    }

    @Test
    public void concurrentApplyForEachTest() throws Exception {
        bigQueue = new BigQueue(testDir, "concurrentApplyForEachTest", BigArray.MINIMUM_DATA_PAGE_SIZE);

        final long N = 100000;

        final Thread publisher = new Thread(new Runnable() {
            private Long item = 1l;

            @Override
            public void run() {
                for (long i = 0; i < N; i++)
                    try {
                        bigQueue.enqueue(item.toString().getBytes());
                        item++;
                        Thread.yield();
                    }
                    catch (final Exception e) {
                        e.printStackTrace();
                    }
            }
        });

        final Thread subscriber = new Thread(new Runnable() {
            private long item = 0l;

            @Override
            public void run() {
                for (long i = 0; i < N; i++)
                    try {
                        if (bigQueue.size() > 0) {
                            final byte[] bytes = bigQueue.dequeue();
                            final String str = new String(bytes);
                            final long curr = Long.parseLong(str);
                            assertEquals(item + 1, curr);
                            item = curr;
                        }

                        Thread.yield();
                    }
                    catch (final Exception e) {
                        e.printStackTrace();
                    }
            }
        });

        subscriber.start();
        publisher.start();

        for (long i = 0; i < N; i += N / 100) {
            final DefaultItemIterator dii = new DefaultItemIterator();
            bigQueue.applyForEach(dii);
            System.out.println("[" + dii.getCount() + "] " + dii.toString());
            Thread.sleep(2);
        }

        publisher.join();
        subscriber.join();
    }

    @Test
    public void testIfFutureIsCompletedAtEnqueueAndListenersAreCalled() throws Exception {
        bigQueue = new BigQueue(testDir, "testIfFutureIsCompletedAtEnqueueAndListenersAreCalled", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final Executor executor1 = mock(Executor.class);
        final Executor executor2 = mock(Executor.class);
        final ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor1);
        future.addListener(mock(Runnable.class), executor2);

        verify(executor1, never()).execute(any(Runnable.class));

        bigQueue.enqueue("test".getBytes());
        bigQueue.enqueue("test2".getBytes());

        assertTrue(future.isDone());
        assertEquals("test", new String(future.get()));
        verify(executor1, times(1)).execute(any(Runnable.class));
        verify(executor2, times(1)).execute(any(Runnable.class));

        final ListenableFuture<byte[]> future2 = bigQueue.dequeueAsync();

        future2.addListener(mock(Runnable.class), executor1);
        assertTrue(future2.isDone());

        try {
            final byte[] entry = future2.get(5, TimeUnit.SECONDS);
            assertEquals("test2", new String(entry));
        }
        catch (final Exception e) {
            fail("Future isn't already completed though there are further entries.");
        }

    }

    @Test
    public void testIfFutureIsCompletedIfListenerIsRegisteredLater() throws Exception {
        bigQueue = new BigQueue(testDir, "testIfFutureIsCompletedIfListenerIsRegisteredLater", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final Executor executor = mock(Executor.class);
        bigQueue.enqueue("test".getBytes());

        final ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        assertNotNull(future);
        future.addListener(mock(Runnable.class), executor);
        verify(executor).execute(any(Runnable.class));

    }

    @Test
    public void testIfFutureIsRecreatedAfterDequeue() throws Exception {
        bigQueue = new BigQueue(testDir, "testIfFutureIsRecreatedAfterDequeue", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final Executor executor = mock(Executor.class);
        bigQueue.enqueue("test".getBytes());
        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        assertTrue(future.isDone());
        future = bigQueue.dequeueAsync();

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());

        future.addListener(mock(Runnable.class), executor);

        bigQueue.enqueue("test".getBytes());
        verify(executor).execute(any(Runnable.class));

    }

    @Test
    public void testIfFutureIsCanceledAfterClosing() throws Exception {
        bigQueue = new BigQueue(testDir, "testIfFutureIsCanceledAfterClosing", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final Executor executor = mock(Executor.class);
        final ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor);
        bigQueue.close();
        assertTrue(future.isCancelled());
    }

    @Test
    public void testIfFutureWorksAfterQueueRecreation() throws Exception {
        bigQueue = new BigQueue(testDir, "testIfFutureWorksAfterQueueRecreation", BigArray.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue("test".getBytes());
        bigQueue.close();

        bigQueue = new BigQueue(testDir, "testIfFutureWorksAfterQueueRecreation", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final Executor executor = mock(Executor.class);
        final ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor);
        assertTrue(future.isDone());
        verify(executor).execute(any(Runnable.class));

    }

    @Test
    public void testIfFutureIsInvalidatedIfAllItemsWhereRemoved() throws Exception {
        bigQueue = new BigQueue(testDir, "testIfFutureIsInvalidatedIfAllItemsWhereRemoved", BigArray.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue("test".getBytes());

        final Executor executor1 = mock(Executor.class);
        final Executor executor2 = mock(Executor.class);

        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor1);

        bigQueue.removeAll();

        future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor2);

        verify(executor1).execute(any(Runnable.class));
        verify(executor2, never()).execute(any(Runnable.class));

        bigQueue.enqueue("test".getBytes());

        verify(executor2).execute(any(Runnable.class));
    }

    @Test
    public void testParallelAsyncDequeueAndPeekOperations() throws Exception {
        bigQueue = new BigQueue(testDir, "testParallelAsyncDequeueAndPeekOperations", BigArray.MINIMUM_DATA_PAGE_SIZE);

        final ListenableFuture<byte[]> dequeueFuture = bigQueue.dequeueAsync();
        final ListenableFuture<byte[]> peekFuture = bigQueue.peekAsync();

        bigQueue.enqueue("Test1".getBytes());

        assertTrue(dequeueFuture.isDone());
        assertTrue(peekFuture.isDone());

        assertEquals("Test1", new String(dequeueFuture.get()));
        assertEquals("Test1", new String(peekFuture.get()));

        assertEquals(0, bigQueue.size());
    }

    @Test
    public void testMultiplePeekAsyncOperations() throws Exception {
        bigQueue = new BigQueue(testDir, "testMultiplePeekAsyncOperations", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final ListenableFuture<byte[]> peekFuture1 = bigQueue.peekAsync();

        bigQueue.enqueue("Test1".getBytes());

        final ListenableFuture<byte[]> peekFuture2 = bigQueue.peekAsync();
        final ListenableFuture<byte[]> peekFuture3 = bigQueue.peekAsync();

        assertTrue(peekFuture1.isDone());
        assertTrue(peekFuture2.isDone());
        assertTrue(peekFuture3.isDone());
        assertEquals(1, bigQueue.size());

        assertEquals("Test1", new String(peekFuture1.get()));
        assertEquals("Test1", new String(peekFuture2.get()));
        assertEquals("Test1", new String(peekFuture3.get()));
    }

    @Test
    @Ignore
    public void testFutureIfConsumerDequeuesAllWhenAsynchronousWriting() throws Exception {
        bigQueue = new BigQueue(testDir, "testFutureIfConsumerDequeuesAllWhenAsynchronousWriting", BigArray.MINIMUM_DATA_PAGE_SIZE);
        final int numberOfItems = 10000;
        final BigQueue spyBigQueue = spy(bigQueue);

        final Executor executor = Executors.newFixedThreadPool(2);
        final Semaphore testFlowControl = new Semaphore(2);
        testFlowControl.acquire(2);

        final ListenableFuture<byte[]> future1 = spyBigQueue.dequeueAsync();

        final Runnable r = new Runnable() {
            int dequeueCount = 0;
            ListenableFuture<byte[]> future;

            @Override
            public void run() {
                byte[] item = null;
                try {
                    item = future == null ? future1.get() : future.get();
                    assertEquals(String.valueOf(dequeueCount), new String(item));
                    dequeueCount++;
                    future = spyBigQueue.dequeueAsync();
                    future.addListener(this, executor);
                    if (dequeueCount == numberOfItems)
                        testFlowControl.release();
                }
                catch (final Exception e) {
                    fail("Unexpected exception during dequeue operation");
                }
            }
        };

        future1.addListener(r, executor);

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfItems; i++)
                    try {
                        spyBigQueue.enqueue(String.valueOf(i).getBytes());
                    }
                    catch (final Exception e) {
                        fail("Unexpected exception during enqueue operation");
                    }
                testFlowControl.release();
            }
        }).start();

        if (testFlowControl.tryAcquire(2, 10, TimeUnit.SECONDS))
            verify(spyBigQueue, times(numberOfItems)).dequeue();
        else
            fail("Something is wrong with the testFlowControl semaphore or timing");
    }

    @After
    public void clean() {
        if (bigQueue != null)
            bigQueue.removeAll();
    }

    private class DefaultItemIterator implements ItemIterator {
        private long count = 0;
        private final StringBuilder sb = new StringBuilder();

        @Override
        public void forEach(final byte[] item) {
            if (count < 20) {
                sb.append(new String(item));
                sb.append(", ");

            }
            count++;
        }

        public long getCount() {
            return count;
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
