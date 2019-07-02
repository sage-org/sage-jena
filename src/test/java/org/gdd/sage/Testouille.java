package org.gdd.sage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryFactory;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.gdd.sage.engine.update.UpdateExecutor;
import org.junit.Test;

public class Testouille {
    @Test
    public void testUpdate() {
        String url = "http://172.16.8.50:8000/sparql/watdiv";
        String query = "INSERT {\n" +
                "  ?v0 <http://schema.org/review> ?v4.\n" +
                "  ?v4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Review> ;\n" +
                "      <https://schema.org/itemReviewed> ?v0 ;\n" +
                "      <https://schema.org/reviewBody> ?v5 ;\n" +
                "      <https://schema.org/creator> ?v6 .\n" +
                "}\n" +
                "WHERE {\n" +
                "  ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/Role0> .\n" +
                "\t?v0\t<http://purl.org/stuff/rev#hasReview>\t?v4 .\n" +
                "\t?v4\t<http://purl.org/stuff/rev#title>\t?v5 .\n" +
                "\t?v4\t<http://purl.org/stuff/rev#reviewer>\t?v6 .\n" +
                "}";
        SageAutoConfiguration factory = new SageAutoConfiguration(url, QueryFactory.create("SELECT * WHERE { ?s ?p ?o}"));
        factory.configure();
        factory.buildDataset();
        Dataset dataset = factory.getDataset();
        UpdateExecutor executor = new UpdateExecutor(url, dataset, 100);
        executor.execute(query);
        dataset.close();
        factory.close();
    }
}
