package org.gdd.sage.engine.update.queries;

import org.apache.jena.sparql.core.Quad;
import org.gdd.sage.engine.update.base.UpdateQuery;

import java.util.List;

public class EmptyQuery implements UpdateQuery {

    /**
     * Build the next SPARQL UPDATE query to execute
     * @return A SPARQL UPDATE query, i.e., either an INSERT DATA or DELETE DATA query
     */
    @Override
    public String nextQuery() {
        return null;
    }

    /**
     * Test if the query has more RDF triples to process
     * @return True the query has more RDF triples to process, False otherwise
     */
    @Override
    public boolean hasNextQuery() {
        return false;
    }

    /**
     * Indicate that a list of quads has been processed by the server
     * @param quads - List of quads
     */
    public void markAsCompleted(List<Quad> quads) {}

    /**
     * Release all resources used by the object for processing
     */
    @Override
    public void close() {}
}
