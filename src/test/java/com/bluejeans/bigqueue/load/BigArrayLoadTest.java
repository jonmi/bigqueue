package com.bluejeans.bigqueue.load;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.bluejeans.bigqueue.BigArray;
import com.bluejeans.bigqueue.TestUtil;

public class BigArrayLoadTest {

    private static String testDir = TestUtil.TEST_BASE_DIR + "bigarray/load";
    private static BigArray bigArray;

    // configurable parameters
    //////////////////////////////////////////////////////////////////
    private static int loop = 5;
    private static int totalItemCount = 100000;
    private static int producerNum = 4;
    private static int consumerNum = 4;
    private static int messageLength = 1024;
    //////////////////////////////////////////////////////////////////

    private static enum Status {
        ERROR, SUCCESS
    }

    private static class Result {
        Status status;
    }

    @After
    public void clean() {
        if (bigArray != null)
            bigArray.removeAll();
    }

    private static final AtomicInteger producingItemCount = new AtomicInteger(0);
    private static final Map<String, AtomicInteger> itemMap = new ConcurrentHashMap<String, AtomicInteger>();

    private static class Producer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;

        public Producer(final CountDownLatch latch, final Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        @Override
        public void run() {
            final Result result = new Result();
            final String rndString = TestUtil.randomString(messageLength);
            try {
                latch.countDown();
                latch.await();

                while (true) {
                    final int count = producingItemCount.incrementAndGet();
                    if (count > totalItemCount)
                        break;
                    final String item = rndString + '-' + count;
                    itemMap.put(item, new AtomicInteger(0));
                    bigArray.append(item.getBytes());
                }
                result.status = Status.SUCCESS;
            }
            catch (final Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }
    }

    // random consumer can only work after producer
    private static class RandomConsumer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;
        private final List<Long> indexList = new ArrayList<Long>();

        public RandomConsumer(final CountDownLatch latch, final Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
            // permute the index to let consumers consume randomly.
            for (long i = 0; i < totalItemCount; i++)
                indexList.add(i);
            Collections.shuffle(indexList);
        }

        @Override
        public void run() {
            final Result result = new Result();
            try {
                latch.countDown();
                latch.await();

                for (final long index : indexList) {

                    final byte[] data = bigArray.get(index);
                    assertNotNull(data);
                    final String item = new String(data);
                    final AtomicInteger counter = itemMap.get(item);
                    assertNotNull(counter);
                    counter.incrementAndGet();
                }
                result.status = Status.SUCCESS;
            }
            catch (final Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }
    }

    // sequential consumer can only work concurrently with producer
    private static class SequentialConsumer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;

        public SequentialConsumer(final CountDownLatch latch, final Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        @Override
        public void run() {
            final Result result = new Result();
            try {
                latch.countDown();
                latch.await();

                for (long index = 0; index < totalItemCount; index++) {
                    while (index >= bigArray.getHeadIndex())
                        Thread.sleep(20); // no item to consume yet, just wait a moment
                    final byte[] data = bigArray.get(index);
                    assertNotNull(data);
                    final String item = new String(data);
                    final AtomicInteger counter = itemMap.get(item);
                    assertNotNull(counter);
                    counter.incrementAndGet();
                }
                result.status = Status.SUCCESS;
            }
            catch (final Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }
    }

    @Test
    public void runTest() throws Exception {
        bigArray = new BigArray(testDir, "load_test");

        System.out.println("Load test begin ...");
        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
            this.doRunProduceThenConsume();

            // reset
            producingItemCount.set(0);
            itemMap.clear();
            bigArray.removeAll();
        }

        bigArray.close();
        bigArray = new BigArray(testDir, "load_test");

        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
            this.doRunMixed();

            // reset
            producingItemCount.set(0);
            itemMap.clear();
            bigArray.removeAll();
        }

        System.out.println("Load test finished successfully.");
    }

    public void doRunProduceThenConsume() throws Exception {
        //prepare
        final CountDownLatch platch = new CountDownLatch(producerNum);
        final CountDownLatch clatch = new CountDownLatch(consumerNum);
        final BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        //run testing
        for (int i = 0; i < producerNum; i++) {
            final Producer p = new Producer(platch, producerResults);
            p.start();
        }

        for (int i = 0; i < producerNum; i++) {
            final Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        bigArray.flush();
        assertTrue(bigArray.size() == totalItemCount);
        assertTrue(itemMap.size() == totalItemCount);

        for (int i = 0; i < consumerNum; i++) {
            final RandomConsumer c = new RandomConsumer(clatch, consumerResults);
            c.start();
        }

        for (int i = 0; i < consumerNum; i++) {
            final Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(bigArray.size() == totalItemCount);
        assertTrue(itemMap.size() == totalItemCount);
        for (final AtomicInteger counter : itemMap.values())
            assertTrue(counter.get() == consumerNum);
    }

    public void doRunMixed() throws Exception {
        //prepare
        final CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        final BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        //run testing
        for (int i = 0; i < producerNum; i++) {
            final Producer p = new Producer(allLatch, producerResults);
            p.start();
        }

        for (int i = 0; i < consumerNum; i++) {
            final SequentialConsumer c = new SequentialConsumer(allLatch, consumerResults);
            c.start();
        }

        //verify
        for (int i = 0; i < producerNum; i++) {
            final Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        for (int i = 0; i < consumerNum; i++) {
            final Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(bigArray.size() == totalItemCount);
        assertTrue(itemMap.size() == totalItemCount);
        for (final AtomicInteger counter : itemMap.values())
            assertTrue(counter.get() == consumerNum);
    }

}
