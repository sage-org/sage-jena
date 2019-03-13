package org.gdd.sage.engine.update.queries;

import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.modify.request.QuadDataAcc;
import org.apache.jena.sparql.modify.request.UpdateDataInsert;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.gdd.sage.engine.update.base.UpdateQueryBase;

import java.util.LinkedList;
import java.util.List;

/**
 * Insert a list of RDF Quads into a RDF dataset
 * @see {@href https://www.w3.org/TR/2013/REC-sparql11-update-20130321/#insertData}
 * @author Thomas Minier
 */
public class InsertQuery extends UpdateQueryBase {

    /**
     * Constructor
     * @param quads - List of RDF Quads to insert
     * @param bucketSize - Bucket size, i.e., how many RDF triples are sent by query
     */
    public InsertQuery(List<Quad> quads, int bucketSize) {
        super(quads, bucketSize);
    }

    /**
     * Build a {@link InsertQuery} from a SPARQL INSERT DATA query
     * @param query - SPARQL INSERT DATA query
     * @param bucketSize  - Bucket size, i.e., how many RDF triples are sent by query
     * @return
     */
    static InsertQuery fromQuery(String query, int bucketSize) {
        List<Quad> quads = new LinkedList<>();
        UpdateRequest plan = UpdateFactory.create(query);
        for(Update op: plan.getOperations()) {
            if(op instanceof UpdateDataInsert) {
                UpdateDataInsert insert = (UpdateDataInsert) op;
                quads.addAll(insert.getQuads());
            }
        }
        return new InsertQuery(quads, bucketSize);
    }

    /**
     * Build a SPARQL UPDATE query from a bucket of quads
     * @param quads - Bucket of quads
     * @return The corresponding SPARQL UPDATE query
     */
    @Override
    protected String buildQuery(List<Quad> quads) {
        UpdateDataInsert insertData = new UpdateDataInsert(new QuadDataAcc(quads));
        UpdateRequest query = new UpdateRequest();
        query.add(insertData);
        return query.toString();
    }

    /**
     * Release all resources used by the object for processing
     */
    @Override
    public void close() {}
}
