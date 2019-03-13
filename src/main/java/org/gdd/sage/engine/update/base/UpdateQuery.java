package org.gdd.sage.engine.update.base;

import org.apache.jena.sparql.core.Quad;

import java.util.List;

/**
 * A generic Update query (INSERT DATA or DELETE DATA)
 * @author Thomas Minier
 */
public interface UpdateQuery {
    /**
     * Build the next SPARQL UPDATE query to execute
     * @return A SPARQL UPDATE query, i.e., either an INSERT DATA or DELETE DATA query
     */
    String nextQuery();

    /**
     * Test if the query has more RDF triples to process
     * @return True the query has more RDF triples to process, False otherwise
     */
    boolean hasNextQuery();

    /**
     * Indicate that a list of quads has been processed by the server
     * @param quads - List of quads
     */
    void markAsCompleted(List<Quad> quads);

    /**
     * Release all resources used by the object for processing
     */
    void close();
}
