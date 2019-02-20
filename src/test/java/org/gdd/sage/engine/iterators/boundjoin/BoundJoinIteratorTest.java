package org.gdd.sage.engine.iterators.boundjoin;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.Utilities;
import org.gdd.sage.engine.iterators.SageBGPIterator;
import org.gdd.sage.http.SageDefaultClient;
import org.gdd.sage.http.SageRemoteClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BoundJoinIteratorTest {
    private SageRemoteClient httpClient;
    private final String GRAPH_URI = "http://sage.univ-nantes.fr/sparql/dbpedia-2015-04en";

    @Before
    public void setUp() {
        httpClient = new SageDefaultClient("http://sage.univ-nantes.fr/sparql");
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    public static QueryIterator getSource(String graphURI, SageRemoteClient httpClient) {
        BasicPattern bgp = new BasicPattern();
        Triple tp = Triple.create(Var.alloc("person"), Utilities.rdf("type"), Utilities.yago("Carpenters"));
        bgp.add(tp);
        return new SageBGPIterator(graphURI, httpClient, bgp);
    }

    @Test
    public void testBoundJoinWithResults() {
        int bucketSize = 15;
        QueryIterator source = getSource(GRAPH_URI, httpClient);
        BasicPattern bgp = new BasicPattern();
        Var joinPos = Var.alloc("person");
        Var label = Var.alloc("label");
        Triple tp = Triple.create(joinPos, Utilities.rdfs("label"), label);
        bgp.add(tp);

        QueryIterator iterator = new BoundJoinIterator(source, GRAPH_URI, httpClient, bgp, bucketSize, null);
        assertTrue("An iterator over a non-empty set of results should have next results", iterator.hasNext());
        int cpt = 0;
        while (iterator.hasNext()) {
            cpt++;
            Binding results = iterator.next();
            assertTrue("The set of results binds ?person", results.contains(joinPos));
            assertTrue("The set of results binds ?label", results.contains(label));
        }
        assertEquals("The iterator should yield 174 solutions mappings", 174, cpt);
        assertFalse("The iterator should not yield any more results", iterator.hasNext());
    }

    @Test
    public void testBoundJoinWithoutResults() {
        int bucketSize = 15;
        QueryIterator source = getSource(GRAPH_URI, httpClient);
        BasicPattern bgp = new BasicPattern();
        Var joinPos = Var.alloc("person");
        Var label = Var.alloc("label");
        Triple tp = Triple.create(joinPos, Utilities.example("toto"), label);
        bgp.add(tp);

        QueryIterator iterator = new BoundJoinIterator(source, GRAPH_URI, httpClient, bgp, bucketSize, null);
        assertFalse("An iterator over an empty set of results should not have results", iterator.hasNext());
    }
}