package org.gdd.sage.engine.update;

import org.apache.jena.sparql.core.Quad;

import java.util.LinkedList;
import java.util.List;

/**
 * A generic Update query (INSERT DATA or DELETE DATA)
 * @author Thomas Minier
 */
public abstract class UpdateQuery {
    private List<Quad> quads;
    private int bucketSize;

    /**
     * Constructor
     * @param quads - List of RDF Quads to process
     * @param bucketSize - Bucket size, i.e., how many RDF triples are sent by query
     */
    public UpdateQuery(List<Quad> quads, int bucketSize) {
        this.bucketSize = bucketSize;
        this.quads = new LinkedList<>();
        this.quads.addAll(quads);
    }

    /**
     * Build a SPARQL UPDATE query from a bucket of quads
     * @param quads - Bucket of quads
     * @return The corresponding SPARQL UPDATE query
     */
    abstract protected String buildQuery(List<Quad> quads);

    /**
     * Delete a RDF quad from the query internal buffer
     * @param quad - Quad to delete
     */
    public void delete(Quad quad) {
        quads.remove(quad);
    }

    /**
     * Delete a list of RDF quads from the query internal buffer
     * @param quads - List of quads to delete
     */
    public void deleteAll(List<Quad> quads) {
        this.quads.removeAll(quads);
    }

    /**
     * Build the next SPARQL UPDATE query to execute
     * @return A SPARQL UPDATE query, i.e., either an INSERT DATA or DELETE DATA query
     */
    public String nextQuery() {
        // gather bucket and build a query from it
        int limit = bucketSize;
        if (limit > quads.size()) {
            limit = quads.size();
        }
        return buildQuery(quads.subList(0, limit));
    }

    /**
     * Test if the query has more RDF triples to process
     * @return True the query has more RDF triples to process, False otherwise
     */
    public boolean hasNextQuery() {
        return !quads.isEmpty();
    }
}
