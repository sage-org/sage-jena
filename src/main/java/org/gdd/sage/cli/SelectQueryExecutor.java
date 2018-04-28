package org.gdd.sage.cli;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;

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
    public void execute(Model model, Query query) {
        if (query.isSelectType()) {
            try(QueryExecution qexec = QueryExecutionFactory.create(query, model) ){
                ResultSet results = qexec.execSelect();
                results = ResultSetFactory.copyResults(results);
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
