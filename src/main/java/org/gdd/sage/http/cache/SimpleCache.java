package org.gdd.sage.http.cache;

import org.gdd.sage.http.results.QueryResults;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Simple LRU cache for SPARQL query results
 * @author Thomas Minier
 */
public class SimpleCache implements QueryCache {
    LRUCache<String, QueryResults> cache;

    private class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private int cacheSize;

        public LRUCache(int cacheSize) {
            super(16);
            this.cacheSize = cacheSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() >= cacheSize;
        }
    }

    /**
     * Constructor
     * @param maxSize - Maximum items to hold in cache
     */
    public SimpleCache(int maxSize) {
        cache = new LRUCache<>(maxSize);
    }

    /**
     * Build the key used to store items in the cache
     * @param graphURI - Default RDF Graph URI
     * @param query - SPARQL query executed
     * @param next - Optional next link sent with the query
     * @return The key used to store items in the cache
     */
    private String buildKey(String graphURI, String query, Optional<String> next) {
        if (next.isPresent()) {
            return "g=" + graphURI + ";q=" + next.get();
        }
        return "g=" + graphURI + ";q=" + query;
    }

    @Override
    public void put(String graphURI, String query, Optional<String> next, QueryResults results) {
        cache.put(buildKey(graphURI, query, next), results);
    }

    @Override
    public boolean has(String graphURI, String query, Optional<String> next) {
        return cache.containsKey(buildKey(graphURI, query, next));
    }

    @Override
    public QueryResults get(String graphURI, String query, Optional<String> next) {
        return cache.get(buildKey(graphURI, query, next));
    }
}
