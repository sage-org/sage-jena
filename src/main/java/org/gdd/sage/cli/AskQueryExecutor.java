package org.gdd.sage.cli;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;

/**
 * Execute a SPARQL ASK query and output results in stdin
 * @author Thomas Minier
 */
public class AskQueryExecutor implements QueryExecutor {
    @Override
    public void execute(Model model, Query query) {
        try(QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSetFormatter.out(qexec.execAsk());
        }
    }
}
