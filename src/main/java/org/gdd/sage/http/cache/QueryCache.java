package org.gdd.sage.http.cache;

import org.gdd.sage.http.data.QueryResults;

import java.util.Optional;

/**
 * Cache interface for SPARQL queries sent to the Sage server
 * @author Thomas Minier
 */
public interface QueryCache {

    /**
     * Put a query in cache
     * @param graphURI - Default RDF Graph URI
     * @param query - SPARQL query executed
     * @param next - Optional next link sent with the query
     * @param results - Query results to put in cache
     */
    void put(String graphURI, String query, Optional<String> next, QueryResults results);

    /**
     * Test if the cache has an entry for this query
     * @param graphURI - Default RDF Graph URI
     * @param query - SPARQL query executed
     * @param next - Optional next link sent with the query
     * @return True if the cache has an entry for this query, False otherwise
     */
    boolean has(String graphURI, String query, Optional<String> next);

    /**
     * Get the cache entry for a query
     * @param graphURI - Default RDF Graph URI
     * @param query - SPARQL query executed
     * @param next - Optional next link sent with the query
     * @return Cache entry
     */
    QueryResults get(String graphURI, String query, Optional<String> next);
}
