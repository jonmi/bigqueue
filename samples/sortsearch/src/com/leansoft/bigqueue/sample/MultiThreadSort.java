package com.leansoft.bigqueue.sample;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import com.leansoft.bigqueue.sample.helper.MergeSortHelper;

/**
 * Sample to sort big data using big queue and multiple threads
 *
 * @author bulldog
 *
 */
public class MultiThreadSort {

    // configurable parameters, adjust them according to your environment and requirements
    //////////////////////////////////////////////////////////////////
    // max number of items can be sorted in memory in one pass
    static int maxInMemSortNumOfItems = 1024 * 1024 * 20;
    // max number of items to be sorted and searched
    static long maxNumOfItems = maxInMemSortNumOfItems * 64;
    // bytes per item
    static int itemSize = 100;
    // ways to merge sort in parallel, must >= 2;
    static int maxMergeSortWays = 4;
    // thread number to sort concurrently
    static int threadNum = 2;
    //////////////////////////////////////////////////////////////////

    public static void main(final String[] args) throws IOException {
        MergeSortHelper.SAMPLE_DIR = MergeSortHelper.SAMPLE_DIR + "multi_thread";

        MergeSortHelper.output("Multi threads sort begin ...");

        MergeSortHelper.output("Generating random big queue ...");
        final IBigQueue srcBigQueue = new BigQueueImpl(MergeSortHelper.SAMPLE_DIR, "srcq");

        final Populator[] populators = new Populator[threadNum];
        for (int i = 0; i < threadNum; i++) {
            populators[i] = new Populator(srcBigQueue, maxNumOfItems, itemSize);
            populators[i].start();
        }
        for (int i = 0; i < threadNum; i++)
            try {
                populators[i].join();
            }
            catch (final InterruptedException e) {
                // ignore
            }

        final long start = System.currentTimeMillis();
        MergeSortHelper.output("Making queue of sorted queues ...");
        final Queue<IBigQueue> queueOfSortedQueues = new LinkedBlockingQueue<IBigQueue>();
        final SortedQueueMaker[] sortedQueueMakers = new SortedQueueMaker[threadNum];
        for (int i = 0; i < threadNum; i++) {
            sortedQueueMakers[i] = new SortedQueueMaker(srcBigQueue, maxInMemSortNumOfItems, queueOfSortedQueues);
            sortedQueueMakers[i].start();
        }
        for (int i = 0; i < threadNum; i++)
            try {
                sortedQueueMakers[i].join();
            }
            catch (final InterruptedException e) {
                // ignore
            }
        srcBigQueue.removeAll();
        srcBigQueue.close();

        MergeSortHelper.output("Merging and sorting the queues ...");
        final MergeSorter[] mergeSorters = new MergeSorter[threadNum];
        for (int i = 0; i < threadNum; i++) {
            mergeSorters[i] = new MergeSorter(queueOfSortedQueues, maxMergeSortWays);
            mergeSorters[i].start();
        }
        for (int i = 0; i < threadNum; i++)
            try {
                mergeSorters[i].join();
            }
            catch (final InterruptedException e) {
                // ignore
            }
        final long end = System.currentTimeMillis();
        MergeSortHelper.output("Mergesort finished.");

        MergeSortHelper.output("Time used to sort " + maxNumOfItems + " string items is " + (end - start) + "ms");
        MergeSortHelper.output("Item size each is " + itemSize + " bytes");
        MergeSortHelper.output("Total size sorted " + maxNumOfItems * itemSize / (1024 * 1024) + "MB");
        MergeSortHelper.output("Thread num " + threadNum);

        final IBigQueue targetSortedQueue = queueOfSortedQueues.poll(); // last and only one is the target sorted queue

        MergeSortHelper.output("Validation begin ....");
        final long targetSize = targetSortedQueue.size();
        if (targetSize != maxNumOfItems)
            System.err.println(
                    "target queue size is not correct!, target queue size is " + targetSize + " expected queue size is " + maxNumOfItems);

        // first sorted item
        String previousItem = new String(targetSortedQueue.dequeue());

        // validate the sorted queue
        for (int i = 1; i < targetSize; i++) {
            final String item = new String(targetSortedQueue.dequeue());
            if (item.compareTo(previousItem) < 0)
                System.err.println("target queue is not in sorted order!");
            previousItem = item;
        }
        MergeSortHelper.output("Validation finished.");

        // have done with target sorted queue, empty it and delete back data files to save disk space
        targetSortedQueue.removeAll();
        targetSortedQueue.close();
    }

    private static final AtomicInteger populatedItemCount = new AtomicInteger(0);

    static void populateBigQueue(final IBigQueue bigQueue, final long maxNumOfItems, final int itemSize) {
        while (true) {
            final int count = populatedItemCount.incrementAndGet();
            if (count > maxNumOfItems)
                break;
            final String randomString = MergeSortHelper.genRandomString(itemSize);
            bigQueue.enqueue(randomString.getBytes());
        }
    }

    // Item populate thread
    static class Populator extends Thread {
        IBigQueue queue;
        long maxNumOfItems;
        int itemSize;

        Populator(final IBigQueue queue, final long maxNumOfItems, final int itemSize) {
            this.queue = queue;
            this.maxNumOfItems = maxNumOfItems;
            this.itemSize = itemSize;
        }

        @Override
        public void run() {
            populateBigQueue(queue, maxNumOfItems, itemSize);
        }
    }

    // Queue of sorted queue making thread
    static class SortedQueueMaker extends Thread {
        Queue<IBigQueue> queueOfSortedQueues;
        IBigQueue srcBigQueue;
        int maxInMemSortNumOfItems;

        SortedQueueMaker(final IBigQueue srcBigQueue, final int maxInMemSortNumOfItems, final Queue<IBigQueue> queueOfSortedQueues) {
            this.srcBigQueue = srcBigQueue;
            this.maxInMemSortNumOfItems = maxInMemSortNumOfItems;
            this.queueOfSortedQueues = queueOfSortedQueues;
        }

        @Override
        public void run() {
            try {
                MergeSortHelper.makeQueueOfSortedQueues(srcBigQueue, maxInMemSortNumOfItems, queueOfSortedQueues);
            }
            catch (final IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    // Merge and sort thread
    static class MergeSorter extends Thread {
        Queue<IBigQueue> queueOfSortedQueues;
        int maxWays;

        MergeSorter(final Queue<IBigQueue> queueOfSortedQueues, final int maxWays) {
            this.queueOfSortedQueues = queueOfSortedQueues;
            this.maxWays = maxWays;
        }

        @Override
        public void run() {
            try {
                MergeSortHelper.mergeSort(queueOfSortedQueues, maxWays);
            }
            catch (final IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
