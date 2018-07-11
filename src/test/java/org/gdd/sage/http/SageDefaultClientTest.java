package org.gdd.sage.http;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.gdd.sage.http.data.QueryResults;
import org.gdd.sage.http.data.SageQueryBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class SageDefaultClientTest {

    private SageRemoteClient client = new SageDefaultClient("http://localhost:8000/sparql/bsbm1M");

    @Test
    @Ignore
    public void buildUnionQuery() {
        String expected = "{\"query\":{\"type\":\"union\",\"union\":[[{\"subject\":\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272\",\"predicate\":\"http://www.w3.org/2000/01/rdf-schema#label\",\"object\":\"?label\"},{\"subject\":\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272\",\"predicate\":\"http://www.w3.org/2000/01/rdf-schema#comment\",\"object\":\"?comment\"}],[{\"subject\":\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272\",\"predicate\":\"http://www.w3.org/2000/01/rdf-schema#label\",\"object\":\"?label\"},{\"subject\":\"http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272\",\"predicate\":\"http://www.w3.org/2000/01/rdf-schema#comment\",\"object\":\"?comment\"}]]},\"next\":null}";
        BasicPattern bgpA = new BasicPattern();
        bgpA.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
                Var.alloc("label")));
        bgpA.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#comment"),
                Var.alloc("comment")));
        BasicPattern bgpB = new BasicPattern();
        bgpB.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
                Var.alloc("label")));
        bgpB.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#comment"),
                Var.alloc("comment")));
        List<BasicPattern> patterns = new ArrayList<>();
        patterns.add(bgpA);
        patterns.add(bgpB);
        String json = SageQueryBuilder.builder()
                .withType("union")
                .withUnion(patterns)
                .build();
        assertEquals(expected, json);
    }

    @Test
    @Ignore
    public void queryUnion() {
        BasicPattern bgpA = new BasicPattern();
        bgpA.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
                Var.alloc("label")));
        bgpA.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#comment"),
                Var.alloc("comment")));
        BasicPattern bgpB = new BasicPattern();
        bgpB.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
                Var.alloc("label")));
        bgpB.add(new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#comment"),
                Var.alloc("comment")));
        List<BasicPattern> patterns = new ArrayList<>();
        patterns.add(bgpA);
        patterns.add(bgpB);

        QueryResults results = client.query(patterns, Optional.empty());
        assertEquals("It should find 2 solution bindings", 2, results.getBindings().size());
        assertFalse("It should not have a next link", results.getNext().isPresent());
    }
}