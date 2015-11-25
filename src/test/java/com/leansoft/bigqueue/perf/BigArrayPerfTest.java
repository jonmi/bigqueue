package com.leansoft.bigqueue.perf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;
import com.leansoft.bigqueue.TestUtil;

public class BigArrayPerfTest {

    private static String testDir = TestUtil.TEST_BASE_DIR + "bigarray/perf";
    private static IBigArray bigArray;

    static {
        bigArray = new BigArrayImpl(testDir, "perf_test");
    }

    // configurable parameters
    //////////////////////////////////////////////////////////////////
    private static int loop = 5;
    private static int totalItemCount = 100000;
    private static int producerNum = 1;
    private static int consumerNum = 1;
    private static int messageLength = 1024;
    //////////////////////////////////////////////////////////////////

    private static enum Status {
        ERROR, SUCCESS
    }

    private static class Result {
        Status status;
        long duration;
    }

    @After
    public void clean() {
        if (bigArray != null)
            bigArray.removeAll();
    }

    private static final AtomicInteger producingItemCount = new AtomicInteger(0);

    private static class Producer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;
        private final byte[] rndBytes = TestUtil.randomString(messageLength).getBytes();

        public Producer(final CountDownLatch latch, final Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        @Override
        public void run() {
            final Result result = new Result();
            try {
                latch.countDown();
                latch.await();

                final long start = System.currentTimeMillis();
                while (true) {
                    final int count = producingItemCount.incrementAndGet();
                    if (count > totalItemCount)
                        break;
                    bigArray.append(rndBytes);
                }
                final long end = System.currentTimeMillis();
                result.status = Status.SUCCESS;
                result.duration = end - start;
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

                final long start = System.currentTimeMillis();
                for (final long index : indexList)
                    bigArray.get(index);
                final long end = System.currentTimeMillis();
                result.status = Status.SUCCESS;
                result.duration = end - start;
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

                final long start = System.currentTimeMillis();
                for (long index = 0; index < totalItemCount; index++) {
                    while (index >= bigArray.getHeadIndex())
                        Thread.sleep(20); // no item to consume yet, just wait a moment
                    @SuppressWarnings("unused")
                    final byte[] data = bigArray.get(index);
                }
                final long end = System.currentTimeMillis();
                result.status = Status.SUCCESS;
                result.duration = end - start;
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
        System.out.println("Performance test begin ...");
        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
            this.doRunProduceThenConsume();

            // reset
            producingItemCount.set(0);
            bigArray.removeAll();
        }

        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
            this.doRunMixed();

            // reset
            producingItemCount.set(0);
            bigArray.removeAll();
        }

        System.out.println("Performance test finished successfully.");
    }

    public void doRunProduceThenConsume() throws Exception {
        //prepare
        final CountDownLatch platch = new CountDownLatch(producerNum);
        final CountDownLatch clatch = new CountDownLatch(consumerNum);
        final BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        long start = System.currentTimeMillis();
        //run testing
        for (int i = 0; i < producerNum; i++) {
            final Producer p = new Producer(platch, producerResults);
            p.start();
        }

        for (int i = 0; i < producerNum; i++) {
            final Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalProducingTime += result.duration;
        }
        long end = System.currentTimeMillis();

        assertTrue(bigArray.size() == totalItemCount);

        System.out.println("-----------------------------------------------");

        System.out.println("Producing test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total items produced = " + totalItemCount);
        System.out.println("Producer thread number = " + producerNum);
        System.out.println("Item message length = " + messageLength + " bytes");
        System.out.println("Total producing time = " + totalProducingTime + " ms.");
        System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
        System.out.println("-----------------------------------------------");

        start = System.currentTimeMillis();
        for (int i = 0; i < consumerNum; i++) {
            final RandomConsumer c = new RandomConsumer(clatch, consumerResults);
            c.start();
        }

        for (int i = 0; i < consumerNum; i++) {
            final Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        end = System.currentTimeMillis();

        System.out.println("-----------------------------------------------");
        System.out.println("Random consuming test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total items consumed = " + totalItemCount);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + messageLength + " bytes");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");

        start = System.currentTimeMillis();
        for (int i = 0; i < consumerNum; i++) {
            final SequentialConsumer c = new SequentialConsumer(clatch, consumerResults);
            c.start();
        }

        for (int i = 0; i < consumerNum; i++) {
            final Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        end = System.currentTimeMillis();

        System.out.println("-----------------------------------------------");
        System.out.println("Sequential consuming test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total items consumed = " + totalItemCount);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + messageLength + " bytes");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");

        assertTrue(bigArray.size() == totalItemCount);
    }

    public void doRunMixed() throws Exception {
        //prepare
        final CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        final BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        final long start = System.currentTimeMillis();
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
            totalProducingTime += result.duration;
        }

        for (int i = 0; i < consumerNum; i++) {
            final Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
            totalConsumingTime += result.duration;
        }

        final long end = System.currentTimeMillis();

        assertTrue(bigArray.size() == totalItemCount);

        System.out.println("-----------------------------------------------");

        System.out.println("Total item count = " + totalItemCount);
        System.out.println("Producer thread number = " + producerNum);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + messageLength + " bytes");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total producing time = " + totalProducingTime + " ms.");
        System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");
    }

}
