package org.gdd.sage.cli;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;

/**
 * Execute a SPARQL DESCRIBE query and output results in stdin
 * @author Thomas Minier
 */
public class DescribeQueryExecutor extends ConstructQueryExecutor {
    public DescribeQueryExecutor(String format) {
        super(format);
    }

    @Override
    protected Model evaluate(Dataset dataset, Query query) {
        try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            return qexec.execDescribe();
        }
    }
}
