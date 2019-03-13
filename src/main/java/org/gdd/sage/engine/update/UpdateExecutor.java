package org.gdd.sage.engine.update;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.modify.request.UpdateDataDelete;
import org.apache.jena.sparql.modify.request.UpdateDataInsert;
import org.apache.jena.sparql.modify.request.UpdateModify;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.gdd.sage.engine.update.base.UpdateQuery;
import org.gdd.sage.engine.update.queries.DeleteInsertQuery;
import org.gdd.sage.engine.update.queries.DeleteQuery;
import org.gdd.sage.engine.update.queries.InsertQuery;
import org.gdd.sage.http.results.UpdateResults;
import org.gdd.sage.model.SageGraph;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Executes SPARQL UPDATE queries.
 * Transparently supports SPARQL INSERT DATA, DELETE DATA and DELETE/INSERT queries.
 * @see {@href https://www.w3.org/TR/2013/REC-sparql11-update-20130321/}
 * @author Thomas Minier
 */
public class UpdateExecutor {
    private String defaultGraphURI;
    private Dataset dataset;
    private SageGraph defaultGraph;
    private int bucketSize;
    private Logger logger;

    /**
     * Constructor
     * @param defaultGraphURI - URI of the default RDF Graph
     * @param dataset - RDF dataset
     * @param bucketSize - Bucket size, i.e., how many RDF triples to process are sent by query
     */
    public UpdateExecutor(String defaultGraphURI,  Dataset dataset, int bucketSize) {
        this.defaultGraphURI = defaultGraphURI;
        this.dataset = dataset;
        // get default graph
        this.defaultGraph = (SageGraph) dataset.asDatasetGraph().getDefaultGraph();
        this.bucketSize = bucketSize;
        logger = ARQ.getExecLogger();
    }

    /**
     * Execute a SPARQL UPDATE query
     * @param query - SPARQL UPDATE query
     */
    public void execute(String query) {
        List<UpdateQuery> updates = new LinkedList<>();

        // parse query and get all update operations in the plan
        UpdateRequest plan = UpdateFactory.create(query);

        for(Update op: plan.getOperations()) {
            if(op instanceof UpdateDataInsert) {
                UpdateDataInsert insert = (UpdateDataInsert) op;
                updates.add(new InsertQuery(insert.getQuads(), bucketSize));
            } else if (op instanceof UpdateDataDelete) {
                UpdateDataDelete delete = (UpdateDataDelete) op;
                updates.add(new DeleteQuery(delete.getQuads(), bucketSize));
            } else if (op instanceof UpdateModify) {
                UpdateModify modify = (UpdateModify) op;
                updates.add(DeleteInsertQuery.fromOperation(modify, dataset, bucketSize));
            }
        }

        // execute each update operation
        for(UpdateQuery update: updates) {
            executeOne(update);
        }
    }

    /**
     * Execute an operation in a SPARQL update query
     * @param update - Operation to execute
     * @return True if the execution was successfull, False otherwise
     */
    private boolean executeOne(UpdateQuery update) {
        // spin until the query has been fully executed
        while (update.hasNextQuery()) {
            // execute query using the HTTP client
            String query = update.nextQuery();
            if (query == null) {
                break;
            }
            UpdateResults results = defaultGraph.getClient().update(defaultGraphURI, query);
            // handle errors
            if (results.hasError()) {
                logger.error(results.getError());
                return false;
            }
            // remove quads that were processed from the update operation
            update.markAsCompleted(results.getProcessedQuads());
        }
        update.close();
        return true;
    }
}
