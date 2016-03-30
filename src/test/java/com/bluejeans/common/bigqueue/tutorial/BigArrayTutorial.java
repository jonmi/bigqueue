package com.bluejeans.common.bigqueue.tutorial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.bluejeans.common.bigqueue.BigArray;

/**
 * A tutorial to show the basic API usage of the big array.
 *
 * @author bulldog
 *
 */
public class BigArrayTutorial {

    @Test
    public void demo() throws IOException {
        BigArray bigArray = null;
        try {
            // create a new big array
            bigArray = new BigArray("d:/bigarray/tutorial", "demo");
            // ensure the new big array is empty
            assertNotNull(bigArray);
            assertTrue(bigArray.isEmpty());
            assertTrue(bigArray.size() == 0);
            assertTrue(bigArray.getHeadIndex() == 0);
            assertTrue(bigArray.getTailIndex() == 0);

            // append some items into the array
            for (int i = 0; i < 10; i++) {
                final String item = String.valueOf(i);
                final long index = bigArray.append(item.getBytes());
                assertTrue(i == index);
            }
            assertTrue(bigArray.size() == 10);
            assertTrue(bigArray.getHeadIndex() == 10);
            assertTrue(bigArray.getTailIndex() == 0);

            // randomly read items in the array
            final String item0 = new String(bigArray.get(0));
            assertEquals(String.valueOf(0), item0);

            final String item3 = new String(bigArray.get(3));
            assertEquals(String.valueOf(3), item3);

            final String item9 = new String(bigArray.get(9));
            assertEquals(String.valueOf(9), item9);

            // empty the big array
            bigArray.removeAll();
            assertTrue(bigArray.isEmpty());
        }
        finally {
            bigArray.close();
        }
    }

}
