package org.gdd.sage;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class Utilities {

    public static Node example(String suffix) {
        return NodeFactory.createURI("http://www.example.org#" + suffix);
    }

    public static Node rdf(String suffix) {
        return NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#" + suffix);
    }

    public static Node rdfs(String suffix) {
        return NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#" + suffix);
    }

    public static Node yago(String suffix) {
        return NodeFactory.createURI("http://dbpedia.org/class/yago/" + suffix);
    }

    public static Node dbo(String suffix) {
        return NodeFactory.createURI("http://dbpedia.org/ontology/" + suffix);
    }

    public static Node dbr(String suffix) {
        return NodeFactory.createURI("http://dbpedia.org/resource/" + suffix);
    }

    public static Node dbp(String suffix) {
        return NodeFactory.createURI("http://dbpedia.org/property/" + suffix);
    }
}
