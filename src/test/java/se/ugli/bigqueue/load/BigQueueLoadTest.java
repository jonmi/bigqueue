package se.ugli.bigqueue.load;

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

import se.ugli.bigqueue.BigQueue;
import se.ugli.bigqueue.TestUtil;

public class BigQueueLoadTest {

    private static String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/load";
    private static BigQueue bigQueue;

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
        if (bigQueue != null)
            bigQueue.removeAll();
    }

    private static final AtomicInteger producingItemCount = new AtomicInteger(0);
    private static final AtomicInteger consumingItemCount = new AtomicInteger(0);
    private static final Set<String> itemSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

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
                    final String item = rndString + count;
                    itemSet.add(item);
                    bigQueue.enqueue(item.getBytes());
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

        public Consumer(final CountDownLatch latch, final Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
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

                    byte[] data = bigQueue.dequeue();
                    while (data == null) {
                        Thread.sleep(10);
                        data = bigQueue.dequeue();
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
        bigQueue = new BigQueue(testDir, "load_test");

        System.out.println("Load test begin ...");
        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
            this.doRunProduceThenConsume();

            // reset
            producingItemCount.set(0);
            consumingItemCount.set(0);
            bigQueue.gc();
        }

        bigQueue.close();
        bigQueue = new BigQueue(testDir, "load_test");

        for (int i = 0; i < loop; i++) {
            System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
            this.doRunMixed();

            // reset
            producingItemCount.set(0);
            consumingItemCount.set(0);
            bigQueue.gc();
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

        assertTrue(!bigQueue.isEmpty());
        assertTrue(bigQueue.size() == totalItemCount);
        bigQueue.flush();

        assertTrue(itemSet.size() == totalItemCount);

        for (int i = 0; i < consumerNum; i++) {
            final Consumer c = new Consumer(clatch, consumerResults);
            c.start();
        }

        for (int i = 0; i < consumerNum; i++) {
            final Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(itemSet.isEmpty());
        assertTrue(bigQueue.isEmpty());
        assertTrue(bigQueue.size() == 0);
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
            final Consumer c = new Consumer(allLatch, consumerResults);
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

        assertTrue(itemSet.isEmpty());
        assertTrue(bigQueue.isEmpty());
        assertTrue(bigQueue.size() == 0);
    }

}
