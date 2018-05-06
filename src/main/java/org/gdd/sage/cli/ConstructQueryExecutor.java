package org.gdd.sage.cli;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

/**
 * Execute a SPARQL CONSTRUCT query and output results in stdin
 * @author Thomas Minier
 */
public class ConstructQueryExecutor implements QueryExecutor {
    private String format;

    public ConstructQueryExecutor(String format) {
        this.format = format;
    }

    @Override
    public void execute(Dataset dataset, Query query) {
        Model resultsModel = evaluate(dataset, query);
        RDFFormat modelFormat;
        switch (format) {
            case "ttl":
            case "turtle":
            case "n3":
                modelFormat = RDFFormat.TURTLE;
                break;
            case "nt":
            case "n-triple":
            case "n-triples":
                modelFormat = RDFFormat.NTRIPLES_UTF8;
                break;
            case "json":
            case "rdf/json":
                modelFormat = RDFFormat.RDFJSON;
                break;
            case "jsonld":
                modelFormat = RDFFormat.JSONLD;
                break;
            case "thrift":
            case "rdf/binary":
                modelFormat = RDFFormat.RDF_THRIFT;
                break;
            default:
                modelFormat = RDFFormat.RDFXML;
                break;
        }
        RDFDataMgr.write(System.out, resultsModel, modelFormat);
    }

    protected Model evaluate(Dataset dataset, Query query) {
        try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            return qexec.execConstruct();
        }
    }
}
