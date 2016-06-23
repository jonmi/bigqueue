package com.bluejeans.bigqueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.bluejeans.bigqueue.BigArray;
import com.bluejeans.bigqueue.FanOutQueue;

public class FanOutQueueTest {

    private final String testDir = TestUtil.TEST_BASE_DIR + "foqueue/unit";
    private FanOutQueue foQueue;

    @Test
    public void simpleTest() throws IOException {
        for (int i = 1; i <= 2; i++) {

            foQueue = new FanOutQueue(testDir, "simple_test");
            assertNotNull(foQueue);

            final String fid = "simpleTest";
            for (int j = 1; j <= 3; j++) {
                assertTrue(foQueue.size(fid) == 0L);
                assertTrue(foQueue.isEmpty(fid));

                assertNull(foQueue.dequeue(fid));
                assertNull(foQueue.peek(fid));

                foQueue.enqueue("hello".getBytes());
                assertTrue(foQueue.size(fid) == 1L);
                assertTrue(!foQueue.isEmpty(fid));
                assertEquals("hello", new String(foQueue.peek(fid)));
                assertEquals("hello", new String(foQueue.dequeue(fid)));
                assertNull(foQueue.dequeue(fid));

                foQueue.enqueue("world".getBytes());
                foQueue.flush();
                assertTrue(foQueue.size(fid) == 1L);
                assertTrue(!foQueue.isEmpty(fid));
                assertEquals("world", new String(foQueue.dequeue(fid)));
                assertNull(foQueue.dequeue(fid));

            }

            foQueue.close();
        }
    }

    @Test
    public void clientManagedIndexTest() {
        foQueue = new FanOutQueue(testDir, "client_managed_index");
        assertNotNull(foQueue);
        assertTrue(foQueue.isEmpty());

        final int loop = 100000;
        for (int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
            assertTrue(foQueue.size() == i + 1L);
            assertTrue(!foQueue.isEmpty());
            assertEquals("" + i, new String(foQueue.get(i)));
        }
    }

    @Test
    public void bigLoopTest() throws IOException {
        foQueue = new FanOutQueue(testDir, "big_loop_test");
        assertNotNull(foQueue);

        final int loop = 100000;
        final String fid1 = "bigLoopTest1";
        long ts = -1;
        for (int i = 0; i < loop; i++) {
            foQueue.enqueue(("" + i).getBytes());
            assertTrue(foQueue.size(fid1) == i + 1L);
            assertTrue(!foQueue.isEmpty(fid1));
            final byte[] data = foQueue.peek(fid1);
            assertEquals("0", new String(data));
            final int length = foQueue.peekLength(fid1);
            assertEquals(1, length);
            if (ts == -1)
                ts = foQueue.peekTimestamp(fid1);
            else
                assertTrue(ts == foQueue.peekTimestamp(fid1));
        }

        assertTrue(foQueue.size(fid1) == loop);
        assertTrue(!foQueue.isEmpty(fid1));
        assertEquals("0", new String(foQueue.peek(fid1)));

        foQueue.close();

        // create a new instance on exiting queue
        foQueue = new FanOutQueue(testDir, "big_loop_test");
        assertTrue(foQueue.size(fid1) == loop);
        assertTrue(!foQueue.isEmpty(fid1));

        for (int i = 0; i < loop; i++) {
            final byte[] data = foQueue.dequeue(fid1);
            assertEquals("" + i, new String(data));
            assertTrue(foQueue.size(fid1) == loop - i - 1);
        }

        assertTrue(foQueue.isEmpty(fid1));

        // fan out test
        final String fid2 = "bigLoopTest2";
        assertTrue(foQueue.size(fid2) == loop);
        assertTrue(!foQueue.isEmpty(fid2));
        assertEquals("0", new String(foQueue.peek(fid2)));

        for (int i = 0; i < loop; i++) {
            final byte[] data = foQueue.dequeue(fid2);
            assertEquals("" + i, new String(data));
            assertTrue(foQueue.size(fid2) == loop - i - 1);
        }

        assertTrue(foQueue.isEmpty(fid2));

        foQueue.close();
    }

    @Test
    public void loopTimingTest() {
        foQueue = new FanOutQueue(testDir, "loop_timing_test");
        assertNotNull(foQueue);

        final String fid1 = "loopTimingTest1";
        final int loop = 1000000;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            foQueue.enqueue(("" + i).getBytes());
        long end = System.currentTimeMillis();
        int timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to enqueue " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            assertEquals("" + i, new String(foQueue.dequeue(fid1)));
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Fanout test 1, Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");

        final String fid2 = "loopTimingTest2";
        begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            assertEquals("" + i, new String(foQueue.dequeue(fid2)));
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Fanout test 2, Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
    }

    @Test
    public void invalidDataPageSizeTest() {
        try {
            foQueue = new FanOutQueue(testDir, "testInvalidDataPageSize", BigArray.MINIMUM_DATA_PAGE_SIZE - 1);
            fail("should throw invalid page size exception");
        }
        catch (final IllegalArgumentException iae) {
            // ecpected
        }
        // ok
        foQueue = new FanOutQueue(testDir, "testInvalidDataPageSize", BigArray.MINIMUM_DATA_PAGE_SIZE);
    }

    @Test
    public void resetQueueFrontIndexTest() {
        foQueue = new FanOutQueue(testDir, "reset_queue_front_index");
        assertNotNull(foQueue);

        final String fid = "resetQueueFrontIndex";
        final int loop = 100000;
        for (int i = 0; i < loop; i++)
            foQueue.enqueue(("" + i).getBytes());

        assertEquals("0", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, 1L);
        assertEquals("1", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, 1234L);
        assertEquals("1234", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, loop - 1);
        assertEquals(loop - 1 + "", new String(foQueue.peek(fid)));

        foQueue.resetQueueFrontIndex(fid, loop);
        assertNull(foQueue.peek(fid));

        try {
            foQueue.resetQueueFrontIndex(fid, loop + 1);
            fail("should throw IndexOutOfBoundsException");
        }
        catch (final IndexOutOfBoundsException e) {
            // expeced
        }
    }

    @Test
    @Ignore
    public void removeBeforeTest() {
        foQueue = new FanOutQueue(testDir, "remove_before", BigArray.MINIMUM_DATA_PAGE_SIZE);

        final String randomString1 = TestUtil.randomString(32);
        for (int i = 0; i < 1024 * 1024; i++)
            foQueue.enqueue(randomString1.getBytes());

        final String fid = "removeBeforeTest";
        assertTrue(foQueue.size(fid) == 1024 * 1024);

        long timestamp = System.currentTimeMillis();
        final String randomString2 = TestUtil.randomString(32);
        for (int i = 0; i < 1024 * 1024; i++)
            foQueue.enqueue(randomString2.getBytes());

        foQueue.removeBefore(timestamp);

        timestamp = System.currentTimeMillis();
        final String randomString3 = TestUtil.randomString(32);
        for (int i = 0; i < 1024 * 1024; i++)
            foQueue.enqueue(randomString3.getBytes());

        foQueue.removeBefore(timestamp);

        assertTrue(foQueue.size(fid) == 2 * 1024 * 1024);
        assertEquals(randomString2, new String(foQueue.peek(fid)));
    }

    @Test
    public void findClosestIndexTest() {
        foQueue = new FanOutQueue(testDir, "find_cloest_index", BigArray.MINIMUM_DATA_PAGE_SIZE);
        assertNotNull(foQueue);

        assertTrue(BigArray.NOT_FOUND == foQueue.findClosestIndex(System.currentTimeMillis()));

        final int loop = 100000;
        final long begin = System.currentTimeMillis();
        TestUtil.sleepQuietly(500);
        for (int i = 0; i < loop; i++)
            foQueue.enqueue(("" + i).getBytes());
        final long midTs1 = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            foQueue.enqueue(("" + i).getBytes());
        final long midTs2 = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            foQueue.enqueue(("" + i).getBytes());

        TestUtil.sleepQuietly(500);
        final long end = System.currentTimeMillis();

        assertTrue(0L == foQueue.findClosestIndex(begin));
        assertTrue(3 * loop - 1 == foQueue.findClosestIndex(end));

        assertTrue(0L == foQueue.findClosestIndex(FanOutQueue.EARLIEST));
        assertTrue(3 * loop == foQueue.findClosestIndex(FanOutQueue.LATEST));

        final long midIndex1 = foQueue.findClosestIndex(midTs1);
        System.out.println("mid index = " + midIndex1);
        final long midIndex2 = foQueue.findClosestIndex(midTs2);
        System.out.println("mid index = " + midIndex2);
        assertTrue(0L < midIndex1);
        assertTrue(midIndex1 < midIndex2);
        assertTrue(3 * loop - 1 > midIndex2);

        final long closestTime = foQueue.getTimestamp(midIndex1);
        final long closestTimeBefore = foQueue.getTimestamp(midIndex1 - 1);
        final long closestTimeAfter = foQueue.getTimestamp(midIndex1 + 1);
        assertTrue(closestTimeBefore <= closestTime);
        assertTrue(closestTimeAfter >= closestTime);
    }

    @Test
    @Ignore
    public void findCloestIndexTest2() {
        foQueue = new FanOutQueue(testDir, "find_cloest_index2", BigArray.MINIMUM_DATA_PAGE_SIZE);
        assertNotNull(foQueue);

        assertTrue(BigArray.NOT_FOUND == foQueue.findClosestIndex(System.currentTimeMillis()));

        final int loop = 100;
        final long[] tsArray = new long[loop];
        for (int i = 0; i < loop; i++) {
            TestUtil.sleepQuietly(10);
            foQueue.enqueue(("" + i).getBytes());
            tsArray[i] = System.currentTimeMillis();
        }

        for (int i = 0; i < loop; i++) {
            final long index = foQueue.findClosestIndex(tsArray[i]);
            assertTrue(index == i);
        }
    }

    @Test
    public void limitBackFileSizeTest() {
        foQueue = new FanOutQueue(testDir, "limit_back_file_size", BigArray.MINIMUM_DATA_PAGE_SIZE);
        assertNotNull(foQueue);

        final int oneM = 1024 * 1024;

        final String randomString1 = TestUtil.randomString(32);
        for (int i = 0; i < oneM; i++)
            foQueue.enqueue(randomString1.getBytes());
        final String randomString2 = TestUtil.randomString(32);
        for (int i = 0; i < oneM; i++)
            foQueue.enqueue(randomString2.getBytes());
        final String randomString3 = TestUtil.randomString(32);
        for (int i = 0; i < oneM; i++)
            foQueue.enqueue(randomString3.getBytes());

        assertEquals(3 * oneM, foQueue.size("test"));
        assertEquals(randomString1, new String(foQueue.dequeue("test")));
        assertEquals(randomString1, new String(foQueue.get(0)));
        assertTrue(6 * 32 * oneM == foQueue.getBackFileSize());

        foQueue.limitBackFileSize(oneM * 4 * 32);
        assertEquals(2 * oneM, foQueue.size("test"));
        assertTrue(4 * 32 * oneM == foQueue.getBackFileSize());
        assertEquals(randomString2, new String(foQueue.dequeue("test")));

        foQueue.limitBackFileSize(oneM * 2 * 32);
        assertEquals(1 * oneM, foQueue.size("test"));
        assertTrue(2 * 32 * oneM == foQueue.getBackFileSize());
        assertEquals(randomString3, new String(foQueue.dequeue("test")));

        foQueue.limitBackFileSize(oneM * 32); // will be ignore
        assertEquals(1 * oneM - 1, foQueue.size("test"));
        assertTrue(2 * 32 * oneM == foQueue.getBackFileSize());
        assertEquals(randomString3, new String(foQueue.dequeue("test")));
    }

    @After
    public void clean() {
        if (foQueue != null)
            foQueue.removeAll();
    }

}
