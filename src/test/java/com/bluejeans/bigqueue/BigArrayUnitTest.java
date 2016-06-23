package com.bluejeans.bigqueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.bluejeans.bigqueue.BigArray;

public class BigArrayUnitTest {

    private final String testDir = TestUtil.TEST_BASE_DIR + "bigarray/unit";
    private BigArray bigArray;

    @Test
    public void simpleTest() {
        bigArray = new BigArray(testDir, "simple_test");
        assertNotNull(bigArray);

        for (int i = 1; i <= 3; i++) {
            assertTrue(bigArray.getTailIndex() == 0L);
            assertTrue(bigArray.getHeadIndex() == 0L);
            assertTrue(bigArray.size() == 0L);
            assertTrue(bigArray.isEmpty());
            assertTrue(!bigArray.isFull());
            try {
                bigArray.get(0);
                fail("IndexOutOfBoundsException should be thrown here");
            }
            catch (final IndexOutOfBoundsException ex) {
            }
            try {
                bigArray.get(1);
                fail("IndexOutOfBoundsException should be thrown here");
            }
            catch (final IndexOutOfBoundsException ex) {
            }
            try {
                bigArray.get(Long.MAX_VALUE);
                fail("IndexOutOfBoundsException should be thrown here");
            }
            catch (final IndexOutOfBoundsException ex) {
            }

            bigArray.append("hello".getBytes());
            assertTrue(bigArray.getTailIndex() == 0L);
            assertTrue(bigArray.getHeadIndex() == 1L);
            assertTrue(bigArray.size() == 1L);
            assertTrue(!bigArray.isEmpty());
            assertTrue(!bigArray.isFull());
            assertEquals("hello", new String(bigArray.get(0)));

            bigArray.flush();

            bigArray.append("world".getBytes());
            assertTrue(bigArray.getTailIndex() == 0L);
            assertTrue(bigArray.getHeadIndex() == 2L);
            assertTrue(bigArray.size() == 2L);
            assertTrue(!bigArray.isEmpty());
            assertTrue(!bigArray.isFull());
            assertEquals("hello", new String(bigArray.get(0)));
            assertEquals("world", new String(bigArray.get(1)));

            bigArray.removeAll();
        }
    }

    @Test
    public void removeBeforeIndexTest() {
        bigArray = new BigArray(testDir, "remove_before_index_test");
        assertNotNull(bigArray);

        final int loop = 5000000;
        for (int i = 0; i < loop; i++)
            bigArray.append(("" + i).getBytes());

        final int half = loop / 2;
        bigArray.removeBeforeIndex(half);
        assertTrue(half == bigArray.getTailIndex());
        assertTrue(half == bigArray.size());
        assertEquals(half + "", new String(bigArray.get(half)));
        assertEquals(half + 1 + "", new String(bigArray.get(half + 1)));
        try {
            bigArray.get(half - 1);
            fail("IndexOutOfBoundsException should be thrown here");
        }
        catch (final IndexOutOfBoundsException ex) {
        }

        final long last = loop - 1;
        bigArray.removeBeforeIndex(last);
        assertTrue(last == bigArray.getTailIndex());
        assertTrue(1 == bigArray.size());
        assertEquals(last + "", new String(bigArray.get(last)));
        try {
            bigArray.get(last - 1);
            fail("IndexOutOfBoundsException should be thrown here");
        }
        catch (final IndexOutOfBoundsException ex) {
        }
    }

    @Test
    public void removeBeforeTest() {
        bigArray = new BigArray(testDir, "remove_before_test");
        assertNotNull(bigArray);

        final int loop = 5000000;
        for (int i = 0; i < loop; i++)
            bigArray.append(("" + i).getBytes());

        final int half = loop / 2;
        final long halfTimestamp = bigArray.getTimestamp(half);
        this.bigArray.removeBefore(halfTimestamp);
        final long tail = this.bigArray.getTailIndex();
        assertTrue(tail > 0);

    }

    @Test
    public void bigLoopTest() throws IOException {
        bigArray = new BigArray(testDir, "big_loop_test");
        assertNotNull(bigArray);

        final int loop = 1000000;
        long lastAppendTime = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            bigArray.append(("" + i).getBytes());
            assertTrue(bigArray.getTailIndex() == 0L);
            assertTrue(bigArray.getHeadIndex() == i + 1L);
            assertTrue(bigArray.size() == i + 1L);
            assertTrue(!bigArray.isEmpty());
            assertTrue(!bigArray.isFull());
            final long currentTime = System.currentTimeMillis();
            final long justAppendTime = bigArray.getTimestamp(i);
            assertTrue(justAppendTime <= currentTime);
            assertTrue(justAppendTime >= lastAppendTime);
            lastAppendTime = justAppendTime;
        }

        try {
            bigArray.get(loop);
            fail("IndexOutOfBoundsException should be thrown here");
        }
        catch (final IndexOutOfBoundsException ex) {
        }

        assertTrue(bigArray.getTailIndex() == 0L);
        assertTrue(bigArray.getHeadIndex() == loop);
        assertTrue(bigArray.size() == loop);
        assertTrue(!bigArray.isEmpty());
        assertTrue(!bigArray.isFull());
        bigArray.close();

        // create a new instance on exiting array
        bigArray = new BigArray(testDir, "big_loop_test");
        assertTrue(bigArray.getTailIndex() == 0L);
        assertTrue(bigArray.getHeadIndex() == loop);
        assertTrue(bigArray.size() == loop);
        assertTrue(!bigArray.isEmpty());
        assertTrue(!bigArray.isFull());

        lastAppendTime = System.currentTimeMillis();
        for (long i = 0; i < 10; i++) {
            bigArray.append(("" + i).getBytes());
            final long currentTime = System.currentTimeMillis();
            final long justAppendTime = bigArray.getTimestamp(loop + i);
            assertTrue(justAppendTime <= currentTime);
            assertTrue(justAppendTime >= lastAppendTime);
            lastAppendTime = justAppendTime;
            assertEquals(i + "", new String(bigArray.get(loop + i)));
        }
        assertTrue(bigArray.getTailIndex() == 0L);
        assertTrue(bigArray.getHeadIndex() == loop + 10);
        assertTrue(bigArray.size() == loop + 10);
        assertTrue(!bigArray.isEmpty());
        assertTrue(!bigArray.isFull());
    }

    @Test
    public void loopTimingTest() {
        bigArray = new BigArray(testDir, "loop_timing_test");
        assertNotNull(bigArray);

        final int loop = 1000000;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            bigArray.append(("" + i).getBytes());
        long end = System.currentTimeMillis();
        int timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to sequentially append " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            assertEquals("" + i, new String(bigArray.get(i)));
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to sequentially read " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        final List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < loop; i++)
            list.add(i);
        Collections.shuffle(list);
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to shuffle " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        for (final int i : list)
            assertEquals("" + i, new String(bigArray.get(i)));
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to randomly read " + loop + " items : " + timeInSeconds + " seconds.");
    }

    @Test
    public void getClosestIndexTest() {
        bigArray = new BigArray(testDir, "get_closest_index_test");
        assertNotNull(bigArray);

        assertTrue(BigArray.NOT_FOUND == bigArray.findClosestIndex(System.currentTimeMillis()));

        final int loop = 2000000;
        final long begin = System.currentTimeMillis();
        TestUtil.sleepQuietly(500);
        for (int i = 0; i < loop; i++)
            bigArray.append(("" + i).getBytes());
        TestUtil.sleepQuietly(500);
        final long end = System.currentTimeMillis();

        final long midTs = (end + begin) / 2;

        assertTrue(0L == bigArray.findClosestIndex(begin));
        assertTrue(loop - 1 == bigArray.findClosestIndex(end));

        final long midIndex = bigArray.findClosestIndex(midTs);
        assertTrue(0L < midIndex);
        assertTrue(loop - 1 > midIndex);

        final long closestTime = bigArray.getTimestamp(midIndex);
        final long closestTimeBefore = bigArray.getTimestamp(midIndex - 1);
        final long closestTimeAfter = bigArray.getTimestamp(midIndex + 1);
        assertTrue(closestTimeBefore <= closestTime);
        assertTrue(closestTimeAfter >= closestTime);
    }

    @Test
    public void getBackFileSizeTest() {
        bigArray = new BigArray(testDir, "get_back_file_size_test");
        assertNotNull(bigArray);

        assertTrue(bigArray.getBackFileSize() == 0);

        bigArray.append("hello".getBytes());
        assertTrue(bigArray.getBackFileSize() == BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize());

        final long loop = 3000000;
        final String randomString = TestUtil.randomString(256);
        for (long i = 0; i < loop; i++)
            bigArray.append(randomString.getBytes());

        long realSize = bigArray.getBackFileSize();
        long expectedSize = BigArray.INDEX_PAGE_SIZE * 3 + bigArray.getDataPageSize() * 6;

        assertTrue(expectedSize == realSize);

        bigArray.removeBeforeIndex(loop / 2);

        realSize = bigArray.getBackFileSize();
        expectedSize = BigArray.INDEX_PAGE_SIZE * 2 + bigArray.getDataPageSize() * 4;

        assertTrue(expectedSize == realSize);

        bigArray.removeAll();

        assertTrue(bigArray.getBackFileSize() == 0);
    }

    @Test
    public void limitBackFileSize() {
        bigArray = new BigArray(testDir, "limit_back_file_size_test");
        assertNotNull(bigArray);

        assertTrue(bigArray.getBackFileSize() == 0);

        bigArray.append("hello".getBytes());
        assertTrue(bigArray.getBackFileSize() == BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize());

        bigArray.limitBackFileSize(bigArray.getDataPageSize()); // no effect
        assertTrue(bigArray.getBackFileSize() == BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize());

        final long loop = 3000000;
        final String randomString = TestUtil.randomString(256);
        for (long i = 0; i < loop; i++)
            bigArray.append(randomString.getBytes());

        bigArray.limitBackFileSize(BigArray.INDEX_PAGE_SIZE * 2 + bigArray.getDataPageSize() * 3);
        assertTrue(bigArray.getBackFileSize() <= BigArray.INDEX_PAGE_SIZE * 2 + bigArray.getDataPageSize() * 3);
        assertTrue(bigArray.getBackFileSize() > BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize() * 2);
        long lastTailIndex = bigArray.getTailIndex();
        assertTrue(lastTailIndex > 0);
        assertTrue(bigArray.getHeadIndex() == loop + 1);

        bigArray.limitBackFileSize(BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize() * 2);
        assertTrue(bigArray.getBackFileSize() <= BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize() * 2);
        assertTrue(bigArray.getBackFileSize() > BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize());
        assertTrue(bigArray.getTailIndex() > lastTailIndex);
        lastTailIndex = bigArray.getTailIndex();
        assertTrue(bigArray.getHeadIndex() == loop + 1);

        bigArray.limitBackFileSize(BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize());
        assertTrue(bigArray.getBackFileSize() == BigArray.INDEX_PAGE_SIZE + bigArray.getDataPageSize());
        assertTrue(bigArray.getTailIndex() > lastTailIndex);
        lastTailIndex = bigArray.getTailIndex();
        assertTrue(bigArray.getHeadIndex() == loop + 1);

        bigArray.append("world".getBytes());

        assertTrue(bigArray.getTailIndex() == lastTailIndex);
        assertTrue(bigArray.getHeadIndex() == loop + 2);
    }

    @Test
    public void getItemLength() {
        bigArray = new BigArray(testDir, "get_data_length_test");
        assertNotNull(bigArray);

        for (int i = 1; i <= 100; i++)
            bigArray.append(TestUtil.randomString(i).getBytes());

        for (int i = 1; i <= 100; i++) {
            final int length = bigArray.getItemLength(i - 1);
            assertTrue(length == i);
        }
    }

    @Test
    public void testInvalidDataPageSize() {
        try {
            bigArray = new BigArray(testDir, "invalid_data_page_size", BigArray.MINIMUM_DATA_PAGE_SIZE - 1);
            fail("should throw invalid page size exception");
        }
        catch (final IllegalArgumentException iae) {
            // ecpected
        }
    }

    @Test
    public void testMinimumDataPageSize() {
        bigArray = new BigArray(testDir, "min_data_page_size", BigArray.MINIMUM_DATA_PAGE_SIZE);

        final String randomString = TestUtil.randomString(BigArray.MINIMUM_DATA_PAGE_SIZE / (1024 * 1024));

        for (int i = 0; i < 1024 * 1024; i++)
            bigArray.append(randomString.getBytes());

        assertTrue(64 * 1024 * 1024 == bigArray.getBackFileSize());

        for (int i = 0; i < 1024 * 1024 * 10; i++)
            bigArray.append(randomString.getBytes());

        assertTrue(11 * 64 * 1024 * 1024 == bigArray.getBackFileSize());

        bigArray.removeBeforeIndex(1024 * 1024);

        assertTrue(10 * 64 * 1024 * 1024 == bigArray.getBackFileSize());

        bigArray.removeBeforeIndex(1024 * 1024 * 2);

        assertTrue(9 * 64 * 1024 * 1024 == bigArray.getBackFileSize());
    }

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testRemoveBeforeIndexDataCorruption() throws InterruptedException, IOException {
        bigArray = new BigArray(tempDir.newFolder().getAbsolutePath(), "data_corruption", BigArray.DEFAULT_DATA_PAGE_SIZE);

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                bigArray.limitBackFileSize(500 * 1024 * 1024);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);

        final int msgSize = 4096;
        final long msgCount = 2L * 1024L * 1024L * 1024L / msgSize;

        System.out.println("msgCount: " + msgCount);
        for (int i = 0; i < msgCount; ++i) {
            final byte[] bytes = new byte[msgSize];
            for (int j = 0; j < msgSize; ++j)
                bytes[j] = 'a';
            bigArray.append(bytes);
        }

        int count = 0;
        for (long i = bigArray.getTailIndex(); i < bigArray.getHeadIndex(); ++i) {
            byte[] bytes = null;
            try {
                bytes = bigArray.get(i);
            }
            catch (final IndexOutOfBoundsException e) {
                System.out.println(i + " is reset to " + bigArray.getTailIndex());
                i = bigArray.getTailIndex();
                continue; // reset
            }
            for (int j = 0; j < msgSize; ++j)
                assertEquals(bytes[j], 'a');
            ++count;
            if (i % 10000 == 0)
                Thread.yield();
        }

        System.out.println(count);
    }

    @After
    public void clean() {
        if (bigArray != null)
            bigArray.removeAll();
    }
}
