package org.gdd.sage.federated;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;

public class Main {
    public static void main(String[] args) {
        BasicPattern bgp = new BasicPattern();
        bgp.add(new Triple(NodeFactory.createURI("http://toto.org"), NodeFactory.createURI("http://rdf.org#type"), NodeFactory.createLiteral("foo")));
        System.out.println(bgp.hashCode());
        bgp.add(new Triple(NodeFactory.createURI("http://toto.org"), NodeFactory.createURI("http://rdf.org#label"), NodeFactory.createLiteral("momo")));
        System.out.println(bgp.hashCode());
    }
}
