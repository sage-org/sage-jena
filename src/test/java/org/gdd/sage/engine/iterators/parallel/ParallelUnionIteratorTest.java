package org.gdd.sage.engine.iterators.parallel;

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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class ParallelUnionIteratorTest {
    private SageRemoteClient httpClient;
    private ExecutorService threadPool;
    private final String GRAPH_URI = "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04";

    @Before
    public void setUp() {
        httpClient = new SageDefaultClient("http://soyez-sage.univ-nantes.fr/sparql");
        threadPool = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
        threadPool.shutdown();
    }

    private QueryIterator getLeft() {
        // search for the label of Michale Jackson
        BasicPattern bgp = new BasicPattern();
        Var label_mj = Var.alloc("label_mj");
        bgp.add(Triple.create(Utilities.dbr("Michael_Jackson"), Utilities.rdfs("label"), label_mj));
        return new SageBGPIterator(GRAPH_URI, httpClient, bgp);
    }

    private QueryIterator getRight() {
        // search for the label of Brad Pitt
        BasicPattern bgp = new BasicPattern();
        Var label_bp = Var.alloc("label_bp");
        bgp.add(Triple.create(Utilities.dbr("Brad_Pitt"), Utilities.rdfs("label"), label_bp));
        return new SageBGPIterator(GRAPH_URI, httpClient, bgp);
    }

    @Test
    public void testParallelUnionWithResults() {
        Var label_mj = Var.alloc("label_mj");
        Var label_bp = Var.alloc("label_bp");
        QueryIterator left = getLeft();
        QueryIterator right = getRight();

        List<Binding> results = new LinkedList<>();
        QueryIterator iterator = new ParallelUnionIterator(threadPool, left, right);

        assertTrue("An iterator with matching results should not be empty", iterator.hasNext());
        iterator.forEachRemaining(results::add);
        assertFalse("An iterator with no remaining results should be empty", iterator.hasNext());

        assertEquals("The union should yield two set of solutions mappings", 2, results.size());
        for(Binding binding: results) {
            assertEquals("Each set of results should have one mapping", 1, binding.size());
            if (binding.contains(label_bp)) {
                assertTrue("The set of results should maps ?label_bj", binding.contains(label_bp));
                assertEquals("The set of results should maps ?label_bj to \"Brad Pitt\"@en", "\"Brad Pitt\"@en", binding.get(label_bp).toString());
                assertFalse("The set of results should not maps ?label_mj", binding.contains(label_mj));
            } else if (binding.contains(label_mj)) {
                assertTrue("The set of results should maps ?label_mj", binding.contains(label_mj));
                assertEquals("The set of results should maps ?label_mj to \"Michael Jackson\"@en", "\"Michael Jackson\"@en", binding.get(label_mj).toString());
                assertFalse("The set of results should not maps ?label_bj", binding.contains(label_bp));
            } else {
                fail("The set of mappings found should binds either ?label_bp or ?label_mj");
            }
        }

    }
}