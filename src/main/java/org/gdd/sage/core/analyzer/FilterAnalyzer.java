package org.gdd.sage.core.analyzer;

import org.apache.jena.graph.Node_Variable;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Collect all filter expressions from a query and aggregate them by variable
 * @author Thomas Minier
 */
public class FilterAnalyzer extends TransformBase {
    private FilterRegistry filters;

    public FilterAnalyzer() {
        filters = new FilterRegistry();
    }

    public FilterRegistry getFilters() {
        return filters;
    }

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        // get filter variables
        ExprList expressions = opFilter.getExprs();
        expressions.forEach(expr -> {
            Set<String> variables = expr.getVarsMentioned().stream().map(Node_Variable::toString).collect(Collectors.toSet());
            filters.put(variables, expr);
        });
        return super.transform(opFilter, subOp);
    }
}
