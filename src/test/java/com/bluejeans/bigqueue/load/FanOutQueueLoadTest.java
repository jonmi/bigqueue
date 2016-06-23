package com.bluejeans.bigqueue.load;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.bluejeans.bigqueue.FanOutQueue;
import com.bluejeans.bigqueue.TestUtil;

public class FanOutQueueLoadTest {

    private static String testDir = TestUtil.TEST_BASE_DIR + "fanout_queue/load";
    private static FanOutQueue foQueue;

    // configurable parameters
    //////////////////////////////////////////////////////////////////
    private static int loop = 5;
    private static int totalItemCount = 100000;
    private static int producerNum = 4;
    private static int consumerGroupANum = 2;
    private static int consumerGroupBNum = 4;
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
        if (foQueue != null)
            foQueue.removeAll();
    }

    private static class Producer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;
        private final AtomicInteger producingItemCount;
        private final Set<String> itemSetA;
        private final Set<String> itemSetB;

        public Producer(final CountDownLatch latch, final Queue<Result> resultQueue, final AtomicInteger producingItemCount,
                final Set<String> itemSetA, final Set<String> itemSetB) {
            this.latch = latch;
            this.resultQueue = resultQueue;
            this.producingItemCount = producingItemCount;
            this.itemSetA = itemSetA;
            this.itemSetB = itemSetB;
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
                    final String item = rndString + count;
                    itemSetA.add(item);
                    itemSetB.add(item);
                    foQueue.enqueue(item.getBytes());
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

    private static class Consumer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;
        private final AtomicInteger consumingItemCount;
        private final String fanoutId;
        private final Set<String> itemSet;

        public Consumer(final CountDownLatch latch, final Queue<Result> resultQueue, final String fanoutId,
                final AtomicInteger consumingItemCount, final Set<String> itemSet) {
            this.latch = latch;
            this.resultQueue = resultQueue;
            this.consumingItemCount = consumingItemCount;
            this.fanoutId = fanoutId;
            this.itemSet = itemSet;
        }

        @Override
        public void run() {
            final Result result = new Result();
            try {
                latch.countDown();
                latch.await();

                while (true) {
                    String item = null;
                    final int index = consumingItemCount.getAndIncrement();
                    if (index >= totalItemCount)
                        break;

                    byte[] data = foQueue.dequeue(fanoutId);
                    while (data == null) {
                        Thread.sleep(10);
                        data = foQueue.dequeue(fanoutId);
                    }
                    item = new String(data);
                    assertNotNull(item);
                    assertTrue(itemSet.remove(item));
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
        foQueue = new FanOutQueue(testDir, "load_test_one");

        System.out.println("Load test begin ...");
        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
            this.doRunProduceThenConsume();

            // reset
            foQueue.removeAll();
        }

        foQueue.close();
        foQueue = new FanOutQueue(testDir, "load_test_two");

        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
            this.doRunMixed();

            // reset
            // reset
            foQueue.removeAll();
        }
        System.out.println("Load test finished successfully.");
    }

    public void doRunMixed() throws Exception {
        final AtomicInteger producerItemCount = new AtomicInteger(0);
        final AtomicInteger consumerGroupAItemCount = new AtomicInteger(0);
        final AtomicInteger consumerGroupBItemCount = new AtomicInteger(0);

        final Set<String> itemSetA = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        final Set<String> itemSetB = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        final String consumerGroupAFanoutId = "groupA";
        final String consumerGroupBFanoutId = "groupB";

        final CountDownLatch platch = new CountDownLatch(producerNum);
        final CountDownLatch clatchGroupA = new CountDownLatch(consumerGroupANum);
        final CountDownLatch clatchGroupB = new CountDownLatch(consumerGroupBNum);
        final BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerGroupAResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerGroupBResults = new LinkedBlockingQueue<Result>();

        //run testing
        for (int i = 0; i < producerNum; i++) {
            final Producer p = new Producer(platch, producerResults, producerItemCount, itemSetA, itemSetB);
            p.start();
        }

        for (int i = 0; i < consumerGroupANum; i++) {
            final Consumer c = new Consumer(clatchGroupA, consumerGroupAResults, consumerGroupAFanoutId, consumerGroupAItemCount, itemSetA);
            c.start();
        }

        for (int i = 0; i < consumerGroupBNum; i++) {
            final Consumer c = new Consumer(clatchGroupB, consumerGroupBResults, consumerGroupBFanoutId, consumerGroupBItemCount, itemSetB);
            c.start();
        }

        for (int i = 0; i < consumerGroupANum; i++) {
            final Result result = consumerGroupAResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        for (int i = 0; i < consumerGroupBNum; i++) {
            final Result result = consumerGroupBResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        for (int i = 0; i < producerNum; i++) {
            final Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(itemSetA.isEmpty());
        assertTrue(foQueue.isEmpty(consumerGroupAFanoutId));
        assertTrue(foQueue.size(consumerGroupAFanoutId) == 0);

        assertTrue(itemSetB.isEmpty());
        assertTrue(foQueue.isEmpty(consumerGroupBFanoutId));
        assertTrue(foQueue.size(consumerGroupBFanoutId) == 0);

        assertTrue(!foQueue.isEmpty());
        assertTrue(foQueue.size() == totalItemCount);
    }

    public void doRunProduceThenConsume() throws Exception {
        //prepare

        final AtomicInteger producerItemCount = new AtomicInteger(0);
        final AtomicInteger consumerGroupAItemCount = new AtomicInteger(0);
        final AtomicInteger consumerGroupBItemCount = new AtomicInteger(0);

        final Set<String> itemSetA = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        final Set<String> itemSetB = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        final String consumerGroupAFanoutId = "groupA";
        final String consumerGroupBFanoutId = "groupB";

        final CountDownLatch platch = new CountDownLatch(producerNum);
        final CountDownLatch clatchGroupA = new CountDownLatch(consumerGroupANum);
        final CountDownLatch clatchGroupB = new CountDownLatch(consumerGroupBNum);
        final BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerGroupAResults = new LinkedBlockingQueue<Result>();
        final BlockingQueue<Result> consumerGroupBResults = new LinkedBlockingQueue<Result>();

        //run testing
        for (int i = 0; i < producerNum; i++) {
            final Producer p = new Producer(platch, producerResults, producerItemCount, itemSetA, itemSetB);
            p.start();
        }

        for (int i = 0; i < producerNum; i++) {
            final Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(!foQueue.isEmpty());
        assertTrue(foQueue.size() == totalItemCount);
        foQueue.flush();

        assertTrue(itemSetA.size() == totalItemCount);
        assertTrue(itemSetB.size() == totalItemCount);

        for (int i = 0; i < consumerGroupANum; i++) {
            final Consumer c = new Consumer(clatchGroupA, consumerGroupAResults, consumerGroupAFanoutId, consumerGroupAItemCount, itemSetA);
            c.start();
        }

        for (int i = 0; i < consumerGroupBNum; i++) {
            final Consumer c = new Consumer(clatchGroupB, consumerGroupBResults, consumerGroupBFanoutId, consumerGroupBItemCount, itemSetB);
            c.start();
        }

        for (int i = 0; i < consumerGroupANum; i++) {
            final Result result = consumerGroupAResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        for (int i = 0; i < consumerGroupBNum; i++) {
            final Result result = consumerGroupBResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(itemSetA.isEmpty());
        assertTrue(foQueue.isEmpty(consumerGroupAFanoutId));
        assertTrue(foQueue.size(consumerGroupAFanoutId) == 0);

        assertTrue(itemSetB.isEmpty());
        assertTrue(foQueue.isEmpty(consumerGroupBFanoutId));
        assertTrue(foQueue.size(consumerGroupBFanoutId) == 0);

        assertTrue(!foQueue.isEmpty());
        assertTrue(foQueue.size() == totalItemCount);
    }

}
