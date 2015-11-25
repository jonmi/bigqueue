package se.ugli.bigqueue;

import java.io.Closeable;

/**
 * FanOut queue ADT
 *
 * @author bulldog
 *
 */
public interface IFanOutQueue extends Closeable {

    /**
     * Constant represents earliest timestamp
     */
    public static final long EARLIEST = -1;
    /**
     * Constant represents latest timestamp
     */
    public static final long LATEST = -2;

    public boolean isEmpty(String fanoutId);

    /**
     * Determines whether the queue is empty
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty();

    /**
     * Adds an item at the back of the queue
     *
     * @param data to be enqueued data
     * @return index where the item was appended
     */
    public long enqueue(byte[] data);

    /**
     * Retrieves and removes the front of a fan out queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */
    public byte[] dequeue(String fanoutId);

    /**
     * Peek the item at the front of a fanout queue, without removing it from the queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */
    public byte[] peek(String fanoutId);

    /**
     * Peek the length of the item at the front of a fan out queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */
    public int peekLength(String fanoutId);

    /**
     * Peek the timestamp of the item at the front of a fan out queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     */
    public long peekTimestamp(String fanoutId);

    /**
     * Retrieves data item at the specific index of the queue
     *
     * @param index data item index
     * @return data at index
     */
    public byte[] get(long index);

    /**
     * Get length of data item at specific index of the queue
     *
     * @param index data item index
     * @return length of data item
     */
    public int getLength(long index);

    /**
     * Get timestamp of data item at specific index of the queue, this is the timestamp when corresponding item was appended into the queue.
     *
     * @param index data item index
     * @return timestamp of data item
     */
    public long getTimestamp(long index);

    public long size(String fanoutId);

    /**
     * Total number of items remaining in the queue.
     *
     * @return total number
     */
    public long size();

    /**
     * Force to persist current state of the queue,
     *
     * normally, you don't need to flush explicitly since:
     * 1.) FanOutQueue will automatically flush a cached page when it is replaced out,
     * 2.) FanOutQueue uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
     *
     * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
     */
    public void flush();

    /**
     * Remove all data before specific timestamp, truncate back files and advance the queue front if necessary.
     *
     * @param timestamp a timestamp
     */
    void removeBefore(long timestamp);

    /**
     * Limit the back file size of this queue, truncate back files and advance the queue front if necessary.
     *
     * Note, this is a best effort call, exact size limit can't be guaranteed
     *
     * @param sizeLmit size limit
     */
    void limitBackFileSize(long sizeLmit);

    /**
     * Current total size of the back files of this queue
     *
     * @return total back file size
     */
    long getBackFileSize();

    /**
     * Find an index closest to the specific timestamp when the corresponding item was enqueued.
     * to find latest index, use {@link #LATEST} as timestamp.
     * to find earliest index, use {@link #EARLIEST} as timestamp.
     *
     * @param timestamp when the corresponding item was appended
     * @return an index
     */
    long findClosestIndex(long timestamp);

    void resetQueueFrontIndex(String fanoutId, long index);

    public void removeAll();

    /**
     * Get the queue front index, this is the earliest appended index
     *
     * @return an index
     */
    public long getFrontIndex();

    public long getFrontIndex(String fanoutId);

    /**
     * Get the queue rear index, this is the next to be appended index
     *
     * @return an index
     */
    public long getRearIndex();

}
