package org.gdd.sage;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.gdd.sage.engine.SageExecutionContext;
import org.gdd.sage.engine.optimizer.OptionalTransformer;
import org.gdd.sage.federated.factory.FederatedQueryFactory;
import org.gdd.sage.federated.factory.ServiceFederatedQueryFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class SageClientTest {

    @Test
    public void publicSageServer() {
        String url = "http://sage.univ-nantes.fr/sparql/dbpedia-2016-04";
        String queryString = "prefix dbpedia-owl: <http://dbpedia.org/ontology/>\n" +
                "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "prefix dbpprop: <http://dbpedia.org/property/>\n" +
                "\n" +
                "SELECT ?name ?deathDate WHERE {\n" +
                "  ?person a dbpedia-owl:Artist;\n" +
                "          rdfs:label ?name;\n" +
                "          dbpedia-owl:birthPlace [ rdfs:label \"York\"@en ].\n" +
                "  OPTIONAL { ?person dbpprop:dateOfDeath ?deathDate. }\n" +
                "}";
        Query query = QueryFactory.create(queryString);
        FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
        factory.buildFederation();
        Transform transformer = new OptionalTransformer();
        query = factory.getLocalizedQuery();
        /*Op op = Algebra.compile(query);
        op = Transformer.transform(transformer, op);*/
        Dataset dataset = factory.getFederationDataset();
        SageExecutionContext.configureDefault(ARQ.getContext());

        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            results.forEachRemaining(System.out::println);
        }
    }
}
