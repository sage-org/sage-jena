package org.gdd.sage.cli;

import org.apache.jena.query.*;

/**
 * Execute a SPARQL SELECT query and output results in stdin
 * @author Thomas Minier
 */
public class SelectQueryExecutor implements QueryExecutor {
    private String format;

    public SelectQueryExecutor(String format) {
        this.format = format;
    }

    @Override
    public void execute(Dataset dataset, Query query) {
        if (query.isSelectType()) {
            try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ){
                ResultSet results = qexec.execSelect();
                switch (format) {
                    case "xml":
                        ResultSetFormatter.outputAsXML(results);
                        break;
                    case "json":
                        ResultSetFormatter.outputAsJSON(results);
                        break;
                    case "csv":
                        ResultSetFormatter.outputAsCSV(results);
                        break;
                    case "tsv":
                        ResultSetFormatter.outputAsTSV(results);
                        break;
                    default:
                        ResultSetFormatter.outputAsSSE(results);
                        break;
                }
            }
        }
    }
}
