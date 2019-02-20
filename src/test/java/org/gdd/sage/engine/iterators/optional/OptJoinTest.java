package org.gdd.sage.engine.iterators.optional;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.Utilities;
import org.gdd.sage.engine.iterators.SageBGPIterator;
import org.gdd.sage.engine.iterators.base.UnionIterator;
import org.gdd.sage.http.SageDefaultClient;
import org.gdd.sage.http.SageRemoteClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class OptJoinTest {
    private SageRemoteClient httpClient;
    private final String GRAPH_URI = "http://sage.univ-nantes.fr/sparql/dbpedia-2016-04";

    @Before
    public void setUp() {
        httpClient = new SageDefaultClient("http://sage.univ-nantes.fr/sparql");
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    public static BasicPattern getLeftNode() {
        BasicPattern bgp = new BasicPattern();
        Triple tp_1 = Triple.create(Var.alloc("actor"), Utilities.rdf("type"), Utilities.dbo("Actor"));
        Triple tp_2 = Triple.create(Var.alloc("actor"), Utilities.dbo("birthPlace"), Var.alloc("birthPlace"));
        Triple tp_3 = Triple.create(Var.alloc("birthPlace"), Utilities.rdfs("label"), NodeFactory.createLiteral("York", "en"));
        bgp.add(tp_1);
        bgp.add(tp_2);
        bgp.add(tp_3);
        return bgp;
    }

    public static BasicPattern getRightNode() {
        BasicPattern bgp = new BasicPattern();
        Triple tp = Triple.create(Var.alloc("actor"), Utilities.dbp("dateOfDeath"), Var.alloc("deathDate"));
        bgp.add(tp);
        return bgp;
    }

    @Test
    public void testOptJoinWithResults() {
        // actors expected to have a death date
        List<String> expected = new LinkedList<>();
        expected.add("http://dbpedia.org/resource/Alan_Webb_(actor)");
        expected.add("http://dbpedia.org/resource/Charles_Prest");
        expected.add("http://dbpedia.org/resource/Eille_Norwood");
        expected.add("http://dbpedia.org/resource/Lyndon_Brook");
        expected.add("http://dbpedia.org/resource/Patrick_Waddington");
        expected.add("http://dbpedia.org/resource/Reginald_Beckwith");

        // sparql variables
        Var actorVar = Var.alloc("actor");
        Var birthPlaceVar = Var.alloc("birthPlace");
        Var deathDateVar = Var.alloc("deathDate");

        // BGPs
        BasicPattern left = getLeftNode();
        left.addAll(getRightNode());
        BasicPattern right = getLeftNode();

        Set<Var> leftVariables = new HashSet<>();
        leftVariables.add(actorVar);
        leftVariables.add(birthPlaceVar);

        Set<Var> joinVariables = new HashSet<>();
        joinVariables.add(actorVar);
        joinVariables.add(birthPlaceVar);
        joinVariables.add(deathDateVar);

        QueryIterator source = new UnionIterator(new SageBGPIterator(GRAPH_URI, httpClient, left), new SageBGPIterator(GRAPH_URI, httpClient, right));
        QueryIterator iterator = new OptJoin(source, leftVariables, joinVariables);
        int cpt = 0;
        assertTrue("An iterator over a non-empty set of results should yield results", iterator.hasNext());
        while(iterator.hasNext()) {
            cpt++;
            Binding binding = iterator.next();
            assertTrue("The variable ?actor should be bound", binding.contains(actorVar));
            assertTrue("The variable ?birthPlace should be bound", binding.contains(birthPlaceVar));
            assertEquals("The variable ?birthPlace should binds to York", "http://dbpedia.org/resource/York", binding.get(birthPlaceVar).toString());
            String actorURI = binding.get(actorVar).toString();
            if (expected.contains(actorURI)) {
                assertTrue("The variable ?deathDate should be bound", binding.contains(deathDateVar));
                expected.remove(actorURI);
            } else {
                assertFalse("The variable ?deathDate should not be bound", binding.contains(deathDateVar));
            }
        }
        assertTrue("All actors with a death date should have been found", expected.isEmpty());
        assertEquals("It should find 22 solutions bindings", 22, cpt);
    }
}