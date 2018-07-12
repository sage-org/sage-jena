package org.gdd.sage.cli;

import org.apache.jena.query.*;

/**
 * Execute a SPARQL ASK query and output results in stdin
 * @author Thomas Minier
 */
public class AskQueryExecutor implements QueryExecutor {
    private String format;

    public AskQueryExecutor(String format) {
        this.format = format;
    }

    @Override
    public void execute(Dataset dataset, Query query) {
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            boolean answer = qexec.execAsk();
            switch (format) {
                case "raw":
                    System.out.println(answer);
                    break;
                case "xml":
                    ResultSetFormatter.outputAsXML(answer);
                    break;
                case "json":
                    ResultSetFormatter.outputAsJSON(answer);
                    break;
                case "csv":
                    ResultSetFormatter.outputAsCSV(answer);
                    break;
                case "tsv":
                    ResultSetFormatter.outputAsTSV(answer);
                    break;
                default:
                    ResultSetFormatter.outputAsSSE(answer);
                    break;
            }
        }
    }
}
