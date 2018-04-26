package org.gdd.sage;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.util.FileManager;

import java.io.InputStream;
import java.util.Iterator;

public class Test {
    public static void main(String[] args) {
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open("/Users/minier-t/Documents/hdt-files/scale1000.ttl");
        if (in == null) {
            throw new IllegalArgumentException("File: not found");
        }
        // model.read(in, null, "TURTLE");

        String queryString = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "\n" +
                "SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3\n" +
                " ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4\n" +
                "WHERE {\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> rdfs:label ?label .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> rdfs:comment ?comment .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:producer ?p .\n" +
                "    ?p rdfs:label ?producer .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> dc:publisher ?p .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productFeature ?f .\n" +
                "    ?f rdfs:label ?productFeature .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual1 ?propertyTextual1 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual2 ?propertyTextual2 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual3 ?propertyTextual3 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric1 ?propertyNumeric1 .\n" +
                "    <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric2 ?propertyNumeric2 .\n" +
                "    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual4 ?propertyTextual4 }\n" +
                "    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual5 ?propertyTextual5 }\n" +
                "    OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric4 ?propertyNumeric4 }\n" +
                "}\n";
        CustomStageGenerator myStageGenerator = new CustomStageGenerator("http://localhost:8000/sparql/bsbm1k");
        StageBuilder.setGenerator(ARQ.getContext(), myStageGenerator) ;
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            results = ResultSetFactory.copyResults(results);
            ResultSetFormatter.out(System.out, results, query);
        }
    }
}
