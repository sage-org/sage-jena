package org.gdd.sage.engine.update.queries;

import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.modify.request.QuadDataAcc;
import org.apache.jena.sparql.modify.request.UpdateDataDelete;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.gdd.sage.engine.update.base.UpdateQueryBase;

import java.util.LinkedList;
import java.util.List;

/**
 * Deletes a list of RDF Quads from a RDF dataset
 * @see {@href https://www.w3.org/TR/2013/REC-sparql11-update-20130321/#deleteData}
 * @author Thomas Minier
 */
public class DeleteQuery extends UpdateQueryBase {

    /**
     * Constructor
     * @param quads - List of RDF Quads to delete
     * @param bucketSize - Bucket size, i.e., how many RDF triples are sent by query
     */
    public DeleteQuery(List<Quad> quads, int bucketSize) {
        super(quads, bucketSize);
    }

    /**
     * Build a {@link DeleteQuery} from a SPARQL DELETE DATA query
     * @param query - SPARQL DELETE DATA query
     * @param bucketSize  - Bucket size, i.e., how many RDF triples are sent by query
     * @return
     */
    static DeleteQuery fromQuery(String query, int bucketSize) {
        List<Quad> quads = new LinkedList<>();
        UpdateRequest plan = UpdateFactory.create(query);
        for(Update op: plan.getOperations()) {
            if(op instanceof UpdateDataDelete) {
                UpdateDataDelete insert = (UpdateDataDelete) op;
                quads.addAll(insert.getQuads());
            }
        }
        return new DeleteQuery(quads, bucketSize);
    }

    /**
     * Build a SPARQL UPDATE query from a bucket of quads
     * @param quads - Bucket of quads
     * @return The corresponding SPARQL UPDATE query
     */
    @Override
    protected String buildQuery(List<Quad> quads) {
        UpdateDataDelete deleteData = new UpdateDataDelete(new QuadDataAcc(quads));
        UpdateRequest query = new UpdateRequest();
        query.add(deleteData);
        return query.toString();
    }

    /**
     * Release all resources used by the object for processing
     */
    @Override
    public void close() {}
}
