package com.leansoft.bigqueue.tutorial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

/**
 * A tutorial to show the basic API usage of the big queue.
 *
 * @author bulldog
 *
 */
public class BigQueueTutorial {

    @Test
    public void demo() throws IOException {

        IBigQueue bigQueue = null;

        try {
            // create a new big queue
            bigQueue = new BigQueueImpl("d:/bigqueue/tutorial", "demo");

            // ensure the new big queue is empty
            assertNotNull(bigQueue);
            assertTrue(bigQueue.size() == 0);
            assertTrue(bigQueue.isEmpty());
            assertNull(bigQueue.dequeue());
            assertNull(bigQueue.peek());

            // enqueue some items
            for (int i = 0; i < 10; i++) {
                final String item = String.valueOf(i);
                bigQueue.enqueue(item.getBytes());
            }
            // now the big queue is not empty
            assertTrue(!bigQueue.isEmpty());
            assertTrue(bigQueue.size() == 10);

            // peek the front of the queue
            assertEquals(String.valueOf(0), new String(bigQueue.peek()));

            // dequeue some items
            for (int i = 0; i < 5; i++) {
                final String item = new String(bigQueue.dequeue());
                assertEquals(String.valueOf(i), item);
            }
            // the big queue is not empty yet
            assertTrue(!bigQueue.isEmpty());
            assertTrue(bigQueue.size() == 5);

            // dequeue all remaining items
            while (true) {
                final byte[] data = bigQueue.dequeue();
                if (data == null)
                    break;
            }
            // now the big is empty since all items have been dequeued
            assertTrue(bigQueue.isEmpty());

        }
        finally {
            // release resources
            bigQueue.close();
        }
    }

}
