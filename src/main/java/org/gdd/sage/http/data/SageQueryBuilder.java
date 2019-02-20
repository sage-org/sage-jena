package org.gdd.sage.http.data;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Builder used to create SPARQL queries that can be send to a SaGe server
 * @author Thomas Minier
 */
public class SageQueryBuilder {

    private SageQueryBuilder() {}

    private static Set<Var> getVariables(BasicPattern bgp) {
        Set<Var> variables = new LinkedHashSet<>();
        for(Triple pattern: bgp.getList()) {
            if (pattern.getSubject().isVariable() && !pattern.getSubject().toString().startsWith("??")) {
                variables.add((Var) pattern.getSubject());
            }
            if (pattern.getPredicate().isVariable() && !pattern.getPredicate().toString().startsWith("??")) {
                variables.add((Var) pattern.getPredicate());
            }
            if (pattern.getObject().isVariable() && !pattern.getObject().toString().startsWith("??")) {
                variables.add((Var) pattern.getObject());
            }
        }
        return variables;
    }

    private static String serializeQuery(Op root) {
        return OpAsQuery.asQuery(root).serialize();
    }

    /**
     * Build a SPARQL query from a Basic graph pattern
     * @param bgp - Basic Graph pattern
     * @return Generated SPARQL query
     */
    public static String buildBGPQuery(BasicPattern bgp) {
        return SageQueryBuilder.buildBGPQuery(bgp, new LinkedList<>());
    }

    /**
     * Build a SPARQL query from a Basic graph pattern and a list of SPARQL filters
     * @param bgp - Basic Graph pattern
     * @param filters - List of SPARQL filters
     * @return Generated SPARQL query
     */
    public static String buildBGPQuery(BasicPattern bgp, List<Expr> filters) {
        // extract SPARQL variables from the BGP
        //Set<Var> variables = SageQueryBuilder.getVariables(bgp);
        // query root: the basic graph pattern itself
        Op op = new OpBGP(bgp);
        // apply SPARQL filters
        for(Expr filter: filters) {
            op = OpFilter.filter(filter, op);
        }
        // apply projection
        //op = new OpProject(op, Lists.newLinkedList(variables));
        return SageQueryBuilder.serializeQuery(op);
    }

    /**
     * Build a SPARQL query from a set of Basic graph patterns
     * @param union - set of Basic Graph patterns
     * @return Generated SPARQL query
     */
    public static String buildUnionQuery(List<BasicPattern> union) {
        // extract SPARQL variables from all BGPs
        /*Set<Var> variables = new LinkedHashSet<>();
        for(BasicPattern bgp: union) {
            variables.addAll(SageQueryBuilder.getVariables(bgp));
        }*/
        // build the union of basic graph patterns
        Op op = new OpBGP(union.get(0));
        for (int index = 1; index < union.size(); index++) {
            op = new OpUnion(op, new OpBGP(union.get(index)));
        }
        // apply projection
        //op = new OpProject(op, Lists.newLinkedList(variables));
        return SageQueryBuilder.serializeQuery(op);
    }
}
