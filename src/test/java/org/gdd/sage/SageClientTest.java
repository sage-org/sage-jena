package org.gdd.sage;

import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Transform;
import org.gdd.sage.engine.SageExecutionContext;
import org.gdd.sage.engine.optimizer.OptionalTransformer;
import org.gdd.sage.federated.factory.FederatedQueryFactory;
import org.gdd.sage.federated.factory.ServiceFederatedQueryFactory;
import org.junit.Test;

public class SageClientTest {

    @Test
    public void publicSageServer() {
        String url = "http://sage.univ-nantes.fr/sparql/bsbm1M";
        String queryString = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>\n" +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "SELECT DISTINCT ?product ?label\n" +
                "WHERE {\n" +
                "    ?product rdfs:label ?label .\n" +
                "    ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType66> .\n" +
                "    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature3> .\n" +
                "    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1967> .\n" +
                "    ?product bsbm:productPropertyNumeric1 ?value1 .\n" +
                "\tFILTER (?value1 > 136)\n" +
                "\t}\n" +
                "ORDER BY ?label\n" +
                "LIMIT 10";
        Query query = QueryFactory.create(queryString);
        FederatedQueryFactory factory = new ServiceFederatedQueryFactory(url, query);
        factory.buildFederation();
        Transform transformer = new OptionalTransformer();
        query = factory.getLocalizedQuery();
        Dataset dataset = factory.getFederationDataset();
        SageExecutionContext.configureDefault(ARQ.getContext());

        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            results.forEachRemaining(System.out::println);
        }
    }
}
