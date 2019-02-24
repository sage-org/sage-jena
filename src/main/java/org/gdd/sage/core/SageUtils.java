package org.gdd.sage.core;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Utilities functions
 * @author Thomas Minier
 */
public class SageUtils {

    private SageUtils() {}

    /**
     * Get the set of SPARQL variables in a triple pattern
     * @param pattern - Triple pattern to analyze
     * @return The set of SPARQL variables in the triple pattern
     */
    public static Set<Var> getVariables(Triple pattern) {
        Set<Var> res = new HashSet<>();
        if(pattern.getSubject().isVariable() && !pattern.getSubject().toString().startsWith("??")) {
            res.add((Var) pattern.getSubject());
        }
        if(pattern.getPredicate().isVariable() && pattern.getPredicate().toString().startsWith("??")) {
            res.add((Var) pattern.getPredicate());
        }
        if(pattern.getObject().isVariable() && !pattern.getObject().toString().startsWith("??")) {
            res.add((Var) pattern.getObject());
        }
        return res;
    }

    /**
     * Get the set of SPARQL variables in a Basic graph pattern
     * @param bgp - Basic graph pattern to analyze
     * @return The set of SPARQL variables in the triple pattern
     */
    public static Set<Var> getVariables(BasicPattern bgp) {
        Set<Var> res = new LinkedHashSet<>();
        for(Triple pattern: bgp) {
            res.addAll(getVariables(pattern));
        }
        return res;
    }
}
