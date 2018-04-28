package org.gdd.sage.cli;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSetFormatter;
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
