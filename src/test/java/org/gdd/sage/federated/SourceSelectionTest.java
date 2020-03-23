package org.gdd.sage.federated;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.gdd.sage.http.SageDefaultClient;
import org.gdd.sage.http.SageRemoteClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SourceSelectionTest {
    private SageRemoteClient httpClient;
    private SourceSelection sourceSelection;
    private final String DBPEDIA_GRAPH_URI = "http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04";
    private final String SAMEAS_GRAPH_URI = "http://soyez-sage.univ-nantes.fr/sparql/sameAs";

    @Before
    public void setUp() throws Exception {
        httpClient = new SageDefaultClient("http://soyez-sage.univ-nantes.fr/sparql");
        sourceSelection = new SourceSelection();
        sourceSelection.registerSource(DBPEDIA_GRAPH_URI, httpClient);
        sourceSelection.registerSource(SAMEAS_GRAPH_URI, httpClient);
    }

    @After
    public void tearDown() throws Exception {
        sourceSelection.close();
    }

    @Test
    public void testLocalizeSimpleBGP() {
        String query = "SELECT * WHERE { <http://dbpedia.org/resource/Albert_Einstein> <http://www.w3.org/2002/07/owl#sameAs> ?cc . ?cc <http://www.w3.org/2000/01/rdf-schema#label> ?name. }";
        Op root = sourceSelection.localize(query);

        assertTrue("The plan root must be an UNION", root instanceof OpUnion);
        OpUnion union = (OpUnion) root;

        // verify right operand
        assertTrue("The right operand of the UNION should be a join ", union.getRight() instanceof OpJoin);
        OpJoin rightOp = (OpJoin) union.getRight();

        // verify right operand of the join
        assertTrue("The left operand of the join should be a SERVICE node", rightOp.getRight() instanceof OpService);
        OpService rightService = (OpService) rightOp.getRight();
        assertEquals("The left SERVICE operand should be localized with <http://soyez-sage.univ-nantes.fr/sparql/sameAs>", SAMEAS_GRAPH_URI, rightService.getService().toString());
        assertTrue("The left SERVICE must wraps a BGP", rightService.getSubOp() instanceof OpBGP);
        OpBGP bgp = (OpBGP) rightService.getSubOp();
        assertEquals("The BGP must be of size 1", 1, bgp.getPattern().size());
        Triple pattern = bgp.getPattern().get(0);
        assertEquals("The pattern's subject must be <http://dbpedia.org/resource/Albert_Einstein>", "http://dbpedia.org/resource/Albert_Einstein", pattern.getSubject().toString());
        assertEquals("The pattern's predicate must be <http://www.w3.org/2002/07/owl#sameAs>", "http://www.w3.org/2002/07/owl#sameAs", pattern.getPredicate().toString());
        assertEquals("The pattern's object must be ?cc", "?cc", pattern.getObject().toString());

        // verify left operand of the join
        assertTrue("The right operand of the join should be a SERVICE node", rightOp.getLeft() instanceof OpService);
        OpService leftService = (OpService) rightOp.getLeft();
        assertEquals("The right SERVICE operand should be localized with <http://soyez-sage.univ-nantes.fr/sparql/dbpedia-2016-04>", DBPEDIA_GRAPH_URI, leftService.getService().toString());
        assertTrue("The right SERVICE must wraps a BGP", leftService.getSubOp() instanceof OpBGP);
        bgp = (OpBGP) leftService.getSubOp();
        assertEquals("The BGP must be of size 1", 1, bgp.getPattern().size());
        pattern = bgp.getPattern().get(0);
        assertEquals("The pattern's subject must be ?cc", "?cc", pattern.getSubject().toString());
        assertEquals("The pattern's predicate must be <http://www.w3.org/2000/01/rdf-schema#label>", "http://www.w3.org/2000/01/rdf-schema#label", pattern.getPredicate().toString());
        assertEquals("The pattern's object must be ?name", "?name", pattern.getObject().toString());

        // verify left operand
        assertTrue("The left operand of the UNION should be a service", union.getLeft() instanceof OpService);
        OpService leftOp = (OpService) union.getLeft();

        bgp = (OpBGP) leftOp.getSubOp();
        assertEquals("The BGP must be of size 2", 2, bgp.getPattern().size());

        // verify first pattern
        pattern = bgp.getPattern().get(0);
        assertEquals("The pattern's subject must be <http://dbpedia.org/resource/Albert_Einstein>", "http://dbpedia.org/resource/Albert_Einstein", pattern.getSubject().toString());
        assertEquals("The pattern's predicate must be <http://www.w3.org/2002/07/owl#sameAs>", "http://www.w3.org/2002/07/owl#sameAs", pattern.getPredicate().toString());
        assertEquals("The pattern's object must be ?cc", "?cc", pattern.getObject().toString());

        // verify second pattern
        pattern = bgp.getPattern().get(1);
        assertEquals("The pattern's subject must be ?cc", "?cc", pattern.getSubject().toString());
        assertEquals("The pattern's predicate must be <http://www.w3.org/2000/01/rdf-schema#label>", "http://www.w3.org/2000/01/rdf-schema#label", pattern.getPredicate().toString());
        assertEquals("The pattern's object must be ?name", "?name", pattern.getObject().toString());
    }
}