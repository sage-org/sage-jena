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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SageUnionIteratorTest {
    private SageRemoteClient httpClient;

    @Before
    public void setUp() {
        httpClient = new SageDefaultClient("http://sage.univ-nantes.fr/sparql");
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    @Test
    public void testUnionWithResults() {
        // search for the label of Michale Jackson
        BasicPattern bgp_1 = new BasicPattern();
        Var label_mj = Var.alloc("label_mj");
        bgp_1.add(Triple.create(Utilities.dbr("Michael_Jackson"), Utilities.rdfs("label"), label_mj));
        // search for the label of Brad Pitt
        BasicPattern bgp_2 = new BasicPattern();
        Var label_bp = Var.alloc("label_bp");
        bgp_2.add(Triple.create(Utilities.dbr("Brad_Pitt"), Utilities.rdfs("label"), label_bp));

        List<BasicPattern> union = new ArrayList<>();
        union.add(bgp_1);
        union.add(bgp_2);

        SageUnionIterator iterator = new SageUnionIterator("http://sage.univ-nantes.fr/sparql/dbpedia-2016-04", httpClient, union);
        assertTrue("An iterator with matching results should not be empty", iterator.hasNext());

        Binding results = iterator.next();
        assertEquals("The first set of results should have one mapping", 1, results.size());
        assertTrue("The first set of results should maps ?label_mj", results.contains(label_mj));
        assertEquals("The first set of results should maps ?label_mj to \"Michael Jackson\"@en", "\"Michael Jackson\"@en", results.get(label_mj).toString());
        assertFalse("The first set of results should not maps ?label_bj", results.contains(label_bp));

        results = iterator.next();
        assertEquals("The second set of results should have one mapping", 1, results.size());
        assertTrue("The first set of results should maps ?label_bj", results.contains(label_bp));
        assertEquals("The second set of results should maps ?label_bj to \"Brad Pitt\"@en", "\"Brad Pitt\"@en", results.get(label_bp).toString());
        assertFalse("The first set of results should not maps ?label_mj", results.contains(label_mj));

        assertFalse("An iterator with no remaining results should be empty", iterator.hasNext());
    }

    @Test
    public void testUnionWithoutResults() {
        // search for the label of Michale Jackson
        BasicPattern bgp_1 = new BasicPattern();
        Var label_mj = Var.alloc("label_1");
        bgp_1.add(Triple.create(Utilities.dbr("Michael_Jacksonnn"), Utilities.rdfs("label"), label_mj));
        // search for the label of Brad Pitt
        BasicPattern bgp_2 = new BasicPattern();
        Var label_bp = Var.alloc("label_2");
        bgp_2.add(Triple.create(Utilities.dbr("Brad_Pitttt"), Utilities.rdfs("label"), label_bp));

        List<BasicPattern> union = new ArrayList<>();
        union.add(bgp_1);
        union.add(bgp_2);

        SageUnionIterator iterator = new SageUnionIterator("http://sage.univ-nantes.fr/sparql/dbpedia-2016-04", httpClient, union);
        assertFalse("An iterator with no matching results should be empty", iterator.hasNext());
    }
}