package org.gdd.sage.cli;

import org.apache.jena.query.*;

/**
 * Execute a SPARQL ASK query and output results in stdin
 * @author Thomas Minier
 */
public class AskQueryExecutor implements QueryExecutor {
    @Override
    public void execute(Dataset dataset, Query query) {
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSetFormatter.out(qexec.execAsk());
        }
    }
}
