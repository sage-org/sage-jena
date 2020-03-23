package org.gdd.sage;

import org.apache.jena.query.*;
import org.gdd.sage.core.factory.SageAutoConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SageClientTest {

    @Parameterized.Parameters(name = "SPARQL query: {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04", "prefix dbo: <http://dbpedia.org/ontology/>\n" +
                        "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "prefix dbp: <http://dbpedia.org/property/>\n" +
                        "\n" +
                        "SELECT ?name ?deathDate WHERE {\n" +
                        "  ?person a dbo:Artist;\n" +
                        "    rdfs:label ?name;\n" +
                        "    dbo:birthPlace [ rdfs:label \"York\"@en ].\n" +
                        "  OPTIONAL { ?person dbp:dateOfDeath ?deathDate. }\n" +
                        "}", 32, "single optional" },
                { "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04", "PREFIX dbp: <http://dbpedia.org/property/>\n" +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "\n" +
                        "SELECT ?titleEng ?title\n" +
                        "WHERE {\n" +
                        "    ?movie rdfs:label ?titleEng, ?title.\n" +
                        "  FILTER LANGMATCHES(LANG(?titleEng), 'EN')\n" +
                        "  FILTER (!LANGMATCHES(LANG(?title), 'EN'))\n" +
                        "} LIMIT 10\n", 10, "Query with two simple filters" },
                { "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04", "PREFIX dbp: <http://dbpedia.org/property/>\n" +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "\n" +
                        "SELECT ?titleEng ?title\n" +
                        "WHERE {\n" +
                        "  ?movie dbp:starring [ rdfs:label 'Natalie Portman'@en ].\n" +
                        "  SERVICE <http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2015-04en> {\n" +
                        "    ?movie rdfs:label ?titleEng, ?title.\n" +
                        "  }\n" +
                        "  FILTER LANGMATCHES(LANG(?titleEng), 'EN')\n" +
                        "  FILTER (!LANGMATCHES(LANG(?title), 'EN'))\n" +
                        "}\n", 218, "federated query n°1 (with bound join)"},
                { "http://soyez-sage.univ-nantes.fr/sparql/swdf-2012", "SELECT * WHERE {\n" +
                        "   ?s <http://www.w3.org/2002/07/owl#sameAs> ?x .\n" +
                        "   ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o1 .\n" +
                        "   SERVICE <http://soyez-sage.univ-nantes.fr/sparql/dbpedia-3-5-1> {\n" +
                        "       ?x <http://dbpedia.org/ontology/thumbnail> ?o2 ." +
                        "   }\n" +
                        "}", 0, "federated query n°2 (no results)"},
                { "http://soyez-sage.univ-nantes.fr/sparql/swdf-2012", "SELECT * WHERE {\n" +
                        "?s <http://www.w3.org/2002/07/owl#sameAs> ?x .\n" +
                        "?x <http://www.w3.org/2000/01/rdf-schema#label> ?o1 .\n" +
                        "SERVICE <http://soyez-sage.univ-nantes.fr/sparql/dbpedia-3-5-1> {\n" +
                        "?x <http://dbpedia.org/ontology/thumbnail> ?o2 . }\n" +
                        "} LIMIT 1\n", 0, "federated query n°3 (no results)"},
                { "http://soyez-sage.univ-nantes.fr/sparql/swdf-2012", "SELECT * WHERE {" +
                        "?s <http://www.w3.org/2002/07/owl#sameAs> ?x . " +
                        "SERVICE <http://soyez-sage.univ-nantes.fr/sparql/dbpedia-3-5-1> { " +
                        "?x <http://dbpedia.org/ontology/knownFor> ?o . } " +
                        "} LIMIT 1", 0, "federated query n°4 (no results)"},
                { "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04", "PREFIX dbp: <http://dbpedia.org/property/>\n" +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "\n" +
                        "SELECT ?title\n" +
                        "WHERE {\n" +
                        "  ?movie dbp:starring [ rdfs:label 'Natalie Portman'@en ].\n" +
                        "  SERVICE <http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2015-04en> {\n" +
                        "    ?movie rdfs:label \"Where the Heart Is (2000 film)\"@en, ?title.\n" +
                        "  }\n" +
                        "  FILTER (!LANGMATCHES(LANG(?title), 'EN'))\n" +
                        "}\n", 11, "federated query n°5 (with hash join)"}
        });
    }

    private String url;
    private String queryString;
    private int numberOfResults;
    private String testName;

    public SageClientTest(String url, String queryString, int numberOfResults, String testName) {
        this.url = url;
        this.queryString = queryString;
        this.numberOfResults = numberOfResults;
        this.testName = testName;
    }

    @Test
    public void testExecuteQuery() {
        Query query = QueryFactory.create(queryString);
        SageAutoConfiguration factory = new SageAutoConfiguration(url, query);
        factory.configure();
        factory.buildDataset();
        query = factory.getQuery();
        Dataset dataset = factory.getDataset();
        try(QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
            ResultSet results = qexec.execSelect();
            List<QuerySolution> solutions = new ArrayList<>();
            results.forEachRemaining(solutions::add);
            assertEquals("It should find " + numberOfResults + " solutions bindings", numberOfResults, solutions.size());
        }
        dataset.close();
        factory.close();
    }
}
