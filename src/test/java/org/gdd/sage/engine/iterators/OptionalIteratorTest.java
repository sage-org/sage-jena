package org.gdd.sage.engine.iterators;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.gdd.sage.Utilities;
import org.gdd.sage.http.SageDefaultClient;
import org.gdd.sage.http.SageRemoteClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class OptionalIteratorTest {
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
        Var joinPos = Var.alloc("person");
        Triple tp_1 = Triple.create(joinPos, Utilities.rdf("type"), Utilities.yago("Carpenters"));
        Triple tp_2 = Triple.create(joinPos, Utilities.rdf("type"), Utilities.yago("PeopleExecutedByCrucifixion"));
        bgp.add(tp_1);
        bgp.add(tp_2);
        return new SageBGPIterator(graphURI, httpClient, bgp);
    }

    @Ignore
    @Test
    public void testOptionalIterator() {
        // install Sage execution context first
        //SageExecutionContext.configureDefault(ARQ.getContext());
        // build optional iterator
        QueryIterator source = getSource(GRAPH_URI, httpClient);
        Var joinPos = Var.alloc("person");
        Var label = Var.alloc("label");
        BasicPattern bgp = new BasicPattern();
        Triple tp = Triple.create(joinPos, Utilities.example("toto"), label);
        bgp.add(tp);
        OpBGP opBGP = new OpBGP(bgp);

        /*QueryIterator iterator = OptionalIterator.create(source, opBGP);
        assertTrue("An iterator over a non-empty set of results should yield results", iterator.hasNext());
        Binding results = iterator.next();
        assertTrue("The results fetched should be an URI", results.get(joinPos).isURI());
        assertEquals("There should be only bindings in the set of solutions", 1, results.size());
        assertEquals("The variable ?person should binds to <\"http://dbpedia.org/resource/Jesus\">", "http://dbpedia.org/resource/Jesus", results.get(joinPos).getURI());
        assertFalse("The variable ?label should not be bound", results.contains(label));
        assertFalse("The iterator should not yield any more results", iterator.hasNext());*/

    }
}