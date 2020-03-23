package org.gdd.sage.engine.iterators;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.Utilities;
import org.gdd.sage.http.SageDefaultClient;
import org.gdd.sage.http.SageRemoteClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SageBGPIteratorTest {
    private SageRemoteClient httpClient;
    private final String GRAPH_URI = "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2015-04en";

    @Before
    public void setUp() {
        httpClient = new SageDefaultClient("http://soyez-sage.univ-nantes.fr/sparql");
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    @Test
    public void testReadBGPWithResults() {
        BasicPattern bgp = new BasicPattern();
        Var joinPos = Var.alloc("person");
        Triple tp_1 = Triple.create(joinPos, Utilities.rdf("type"), Utilities.yago("Carpenters"));
        Triple tp_2 = Triple.create(joinPos, Utilities.rdf("type"), Utilities.yago("PeopleExecutedByCrucifixion"));
        bgp.add(tp_1);
        bgp.add(tp_2);
        SageBGPIterator iterator = new SageBGPIterator(GRAPH_URI, httpClient, bgp);
        assertTrue("An iterator over a non-empty set of results should have next results", iterator.hasNext());
        Binding results = iterator.next();
        assertTrue("The results fetched should be an URI", results.get(joinPos).isURI());
        assertEquals("There should be only bindings in the set of solutions", 1, results.size());
        assertEquals("The only URI fetched should be <\"http://dbpedia.org/resource/Jesus\">", "http://dbpedia.org/resource/Jesus", results.get(joinPos).getURI());
        assertFalse("The iterator should not yield any more results", iterator.hasNext());
    }

    @Test
    public void TestNoResults() {
        BasicPattern bgp = new BasicPattern();
        Triple tp = Triple.create(Var.alloc("person"), Utilities.rdf("type"), Utilities.yago("BarackObama"));
        bgp.add(tp);
        SageBGPIterator iterator = new SageBGPIterator(GRAPH_URI, httpClient, bgp);
        assertFalse("An iterator over a BGP with no matches should be empty", iterator.hasNext());
    }


}