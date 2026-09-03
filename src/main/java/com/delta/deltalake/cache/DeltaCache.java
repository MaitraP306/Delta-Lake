
package com.delta.deltalake.cache;

import java.util.LinkedHashMap;
import java.util.Objects;

public final class DeltaCache<K, V> {
    private final int capacity;
    private final LinkedHashMap<K, V> entries;

    public DeltaCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.entries = new LinkedHashMap<>(16, 0.75f, true);
    }

    public synchronized V get(K key) {
        return entries.get(key);
    }

    public synchronized void put(K key, V value) {
        entries.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
        if (entries.size() > capacity) {
            entries.remove(entries.keySet().iterator().next());
        }
    }

    public synchronized void remove(K key) {
        entries.remove(key);
    }

    public synchronized void clear() {
        entries.clear();
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized boolean containsKey(K key) {
        return entries.containsKey(key);
    }
}
