package org.gdd.sage.engine.update;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.modify.request.UpdateDataDelete;
import org.apache.jena.sparql.modify.request.UpdateDataInsert;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.results.UpdateResults;
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
    private SageRemoteClient httpClient;
    private int bucketSize;
    private Logger logger;

    /**
     * Constructor
     * @param defaultGraphURI - URI of the default RDF Graph
     * @param httpClient - HTTP client used to send SPARQL queries to the server
     * @param bucketSize - Bucket size, i.e., how many RDF triples to process are sent by query
     */
    public UpdateExecutor(String defaultGraphURI, SageRemoteClient httpClient, int bucketSize) {
        this.defaultGraphURI = defaultGraphURI;
        this.httpClient = httpClient;
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
            }
            // TODO handle INSERT/DELETE queries
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
            UpdateResults results = httpClient.update(defaultGraphURI, update);
            // handle errors
            if (results.hasError()) {
                logger.error(results.getError());
                return false;
            }
            // remove quads that were processed from the update operation
            update.deleteAll(results.getProcessedQuads());
        }
        return true;
    }
}
