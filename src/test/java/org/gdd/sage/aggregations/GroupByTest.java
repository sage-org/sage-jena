package org.gdd.sage.aggregations;

import org.apache.jena.query.*;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.List;

public class GroupByTest {

    @Ignore
    public static void main(String[] args) {
        String url = "http://localhost:8000/sparql/bsbm1k";
        String queryString = "SELECT (COUNT(?p) AS ?x) ?c WHERE { ?s a ?c ; ?p ?o } GROUP BY ?c";

        Query query = QueryFactory.create(queryString);
        SageAutoConfiguration factory = new SageAutoConfiguration(url, query);
        factory.configure();
        factory.buildDataset();
        query = factory.getQuery();
        Dataset dataset = factory.getDataset();
        float startTime = System.nanoTime();
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            List<QuerySolution> solutions = new ArrayList<>();
            results.forEachRemaining(r -> {
                solutions.add(r);
                System.out.println(r);
            });
            float endTime = System.nanoTime();
            System.out.println("Executed in " + ((endTime - startTime) / 1e9) + "s");
            System.out.println(solutions.size() + " groups generated");
        }
        dataset.close();
        factory.close();
    }
}
