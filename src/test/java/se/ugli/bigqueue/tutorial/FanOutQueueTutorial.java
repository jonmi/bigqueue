package se.ugli.bigqueue.tutorial;

import java.io.IOException;

import org.junit.Test;

import se.ugli.bigqueue.FanOutQueue;

/**
 * A tutorial to show the basic API usage of the fanout queue.
 *
 * @author bulldog
 *
 */
public class FanOutQueueTutorial {

    @Test
    public void demo() throws IOException {
        FanOutQueue foQueue = null;

        try {
            // create a new fanout queue
            foQueue = new FanOutQueue("d:/tutorial/fanout-queue", "demo");

            // enqueue some logs
            for (int i = 0; i < 10; i++) {
                final String log = "log-" + i;
                foQueue.enqueue(log.getBytes());
            }

            // consuming the queue with fanoutId 1
            final String fanoutId1 = "realtime";
            System.out.println("output from " + fanoutId1 + " consumer:");
            while (!foQueue.isEmpty(fanoutId1)) {
                final String item = new String(foQueue.dequeue(fanoutId1));
                System.out.println(item);
            }

            // consuming the queue with fanoutId 2
            final String fanoutId2 = "offline";
            System.out.println("output from " + fanoutId2 + " consumer:");
            while (!foQueue.isEmpty(fanoutId2)) {
                final String item = new String(foQueue.dequeue(fanoutId2));
                System.out.println(item);
            }
        }
        finally {
            // release resource
            if (foQueue != null)
                foQueue.close();
        }
    }

}
