package se.ugli.bigqueue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

/**
 * Simple and thread-safe LRU cache implementation,
 * supporting time to live and reference counting for entry.
 *
 * in current implementation, entry expiration and purge(mark and sweep) is triggered by put operation,
 * and resource closing after mark and sweep is done in async way.
 *
 * @author bulldog
 *
 * @param <K> key
 * @param <V> value
 */
class LRUCache<K, V extends Closeable> {

    private final static Logger logger = Logger.getLogger(LRUCache.class);

    public static final long DEFAULT_TTL = 10 * 1000; // milliseconds

    private final Map<K, V> map;
    private final Map<K, TTLValue> ttlMap;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    private final Set<K> keysToRemove = new HashSet<K>();

    public LRUCache() {
        map = new HashMap<K, V>();
        ttlMap = new HashMap<K, TTLValue>();
    }

    /**
     * Put a keyed resource with specific ttl into the cache
     *
     * This call will increment the reference counter of the keyed resource.
     *
     * @param key the key of the cached resource
     * @param value the to be cached resource
     * @param ttlInMilliSeconds time to live in milliseconds
     */

    public void put(final K key, final V value, final long ttlInMilliSeconds) {
        Collection<V> valuesToClose = null;
        try {
            writeLock.lock();
            // trigger mark&sweep
            valuesToClose = markAndSweep();
            if (valuesToClose != null && valuesToClose.contains(value))
                valuesToClose.remove(value);
            map.put(key, value);
            final TTLValue ttl = new TTLValue(System.currentTimeMillis(), ttlInMilliSeconds);
            ttl.refCount.incrementAndGet();
            ttlMap.put(key, ttl);
        }
        finally {
            writeLock.unlock();
        }
        if (valuesToClose != null && valuesToClose.size() > 0) {
            if (logger.isDebugEnabled()) {
                final int size = valuesToClose.size();
                logger.info("Mark&Sweep found " + size + (size > 1 ? " resources" : " resource") + " to close.");
            }
            // close resource asynchronously
            executorService.execute(new ValueCloser<V>(valuesToClose));
        }
    }

    /**
     * Put a keyed resource with default ttl into the cache
     *
     * This call will increment the reference counter of the keyed resource.
     *
     * @param key the key of the cached resource
     * @param value the to be cached resource
     */

    public void put(final K key, final V value) {
        this.put(key, value, DEFAULT_TTL);
    }

    /**
     * A lazy mark and sweep,
     *
     * a separate thread can also do this.
     */
    private Collection<V> markAndSweep() {
        Collection<V> valuesToClose = null;
        keysToRemove.clear();
        final Set<K> keys = ttlMap.keySet();
        final long currentTS = System.currentTimeMillis();
        for (final K key : keys) {
            final TTLValue ttl = ttlMap.get(key);
            if (ttl.refCount.get() <= 0 && currentTS - ttl.lastAccessedTimestamp.get() > ttl.ttl)
                keysToRemove.add(key);
        }

        if (keysToRemove.size() > 0) {
            valuesToClose = new HashSet<V>();
            for (final K key : keysToRemove) {
                final V v = map.remove(key);
                valuesToClose.add(v);
                ttlMap.remove(key);
            }
        }

        return valuesToClose;
    }

    /**
     * Get a cached resource with specific key
     *
     * This call will increment the reference counter of the keyed resource.
     *
     * @param key the key of the cached resource
     * @return cached resource if exists
     */

    public V get(final K key) {
        try {
            readLock.lock();
            final TTLValue ttl = ttlMap.get(key);
            if (ttl != null) {
                // Since the resource is acquired by calling thread,
                // let's update last accessed timestamp and increment reference counting
                ttl.lastAccessedTimestamp.set(System.currentTimeMillis());
                ttl.refCount.incrementAndGet();
            }
            return map.get(key);
        }
        finally {
            readLock.unlock();
        }
    }

    private static class TTLValue {
        AtomicLong lastAccessedTimestamp; // last accessed time
        AtomicLong refCount = new AtomicLong(0);
        long ttl;

        public TTLValue(final long ts, final long ttl) {
            this.lastAccessedTimestamp = new AtomicLong(ts);
            this.ttl = ttl;
        }
    }

    private static class ValueCloser<V extends Closeable> implements Runnable {
        Collection<V> valuesToClose;

        public ValueCloser(final Collection<V> valuesToClose) {
            this.valuesToClose = valuesToClose;
        }

        @Override
        public void run() {
            final int size = valuesToClose.size();
            for (final V v : valuesToClose)
                if (v != null)
                    CloseCommand.close(v);
            if (logger.isDebugEnabled())
                logger.debug("ResourceCloser closed " + size + (size > 1 ? " resources." : " resource."));
        }
    }

    public void release(final K key) {
        try {
            readLock.lock();
            final TTLValue ttl = ttlMap.get(key);
            if (ttl != null)
                // since the resource is released by calling thread
                // let's decrement the reference counting
                ttl.refCount.decrementAndGet();
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * The size of the cache, equals to current total number of cached resources.
     *
     * @return the size of the cache
     */

    public int size() {
        try {
            readLock.lock();
            return map.size();
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Remove all cached resource from the cache and close them asynchronously afterwards.
     */

    public void removeAll() {
        try {
            writeLock.lock();

            final Collection<V> valuesToClose = new HashSet<V>();
            valuesToClose.addAll(map.values());

            if (valuesToClose != null && valuesToClose.size() > 0)
                // close resource synchronously
                for (final V v : valuesToClose)
                    CloseCommand.close(v);
            map.clear();
            ttlMap.clear();

        }
        finally {
            writeLock.unlock();
        }

    }

    /**
     * Remove the resource with specific key from the cache and close it synchronously afterwards.
     *
     * @param key the key of the cached resource
     * @return the removed resource if exists
     */

    public V remove(final K key) {
        try {
            writeLock.lock();
            ttlMap.remove(key);
            final V value = map.remove(key);
            if (value != null)
                // close synchronously
                CloseCommand.close(value);
            return value;
        }
        finally {
            writeLock.unlock();
        }

    }

    /**
     * All values cached
     * @return a collection
     */

    public Collection<V> getValues() {
        try {
            readLock.lock();
            final Collection<V> col = new ArrayList<V>();
            for (final V v : map.values())
                col.add(v);
            return col;
        }
        finally {
            readLock.unlock();
        }
    }

}
