package org.gdd.sage;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.util.FileManager;
import org.gdd.sage.engine.SageStageGenerator;
import org.gdd.sage.model.SageModelFactory;

import java.io.IOException;
import java.io.InputStream;

public class Test {
    public static void main(String[] args) {
        Model model = SageModelFactory.createModel("http://localhost:8000/sparql/bsbm1k");
        /*try (InputStream in = FileManager.get().open("/Users/minier-t/Documents/hdt-files/scale1000.ttl")) {
            //model.read(in, null, "TURTLE");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        String queryString = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX rev: <http://purl.org/stuff/rev#>\n" +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" +
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "\n" +
                "SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle\n" +
                "       ?reviewer ?revName ?rating1 ?rating2\n" +
                "WHERE {\n" +
                "\t<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> rdfs:label ?productLabel .\n" +
                "    OPTIONAL {\n" +
                "        ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> .\n" +
                "\t\t?offer bsbm:price ?price .\n" +
                "\t\t?offer bsbm:vendor ?vendor .\n" +
                "\t\t?vendor rdfs:label ?vendorTitle .\n" +
                "        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .\n" +
                "        ?offer dc:publisher ?vendor .\n" +
                "        ?offer bsbm:validTo ?date .\n" +
                "        FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )\n" +
                "    }\n" +
                "    OPTIONAL {\n" +
                "\t?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> .\n" +
                "\t?review rev:reviewer ?reviewer .\n" +
                "\t?reviewer foaf:name ?revName .\n" +
                "\t?review dc:title ?revTitle .\n" +
                "    OPTIONAL { ?review bsbm:rating1 ?rating1 . }\n" +
                "    OPTIONAL { ?review bsbm:rating2 ?rating2 . }\n" +
                "    }\n" +
                "}\n";
        SageStageGenerator myStageGenerator = new SageStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(), myStageGenerator) ;
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            ResultSetFormatter.out(System.out, results, query);
        }
    }
}
