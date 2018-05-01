package org.gdd.sage.http;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simple LRU cache implemented on top of a LinkedHashMap
 * @param <K> - Type of the keys
 * @param <V> - Type of the values
 * @author Thomas Minier
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int cacheSize;

    public LRUCache(int cacheSize) {
        super();
        this.cacheSize = cacheSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        LRUCache<?, ?> lruCache = (LRUCache<?, ?>) o;
        return cacheSize == lruCache.cacheSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cacheSize);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }
}
