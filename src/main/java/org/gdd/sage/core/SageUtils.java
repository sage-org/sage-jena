package org.gdd.sage.core;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;

import java.util.LinkedList;
import java.util.List;

public class SageUtils {

    public static List<Var> getVariables(Triple triple) {
        List<Var> variables = new LinkedList<>();
        if (triple.getSubject().isVariable()) {
            variables.add((Var) triple.getSubject());
        }
        if (triple.getPredicate().isVariable()) {
            variables.add((Var) triple.getPredicate());
        }
        if (triple.getObject().isVariable()) {
            variables.add((Var) triple.getObject());
        }
        return variables;
    }

    public static List<Var> getVariables(BasicPattern bgp) {
        List<Var> variables = new LinkedList<>();
        for(Triple triple: bgp.getList()) {
            variables.addAll(getVariables(triple));
        }
        return variables;
    }
}
