package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * A reducer used to reconstruct MIN aggregation
 * @author Thomas Minier
 */
public class MinReducer extends UnaryReducer {
    public MinReducer(Var variable, ExecutionContext context) {
        super(variable, context);
    }

    @Override
    NodeValue bottom() {
        return null;
    }

    @Override
    NodeValue merge(NodeValue x, NodeValue y) {
        // base case: the first y value is the new min
        if (x == null) {
            return y;
        }

        // compare values
        if (NodeValue.compareAlways(x, y) > 0) {
            return y;
        }
        return x;
    }

    @Override
    NodeValue reduce(NodeValue v) {
        return v;
    }
}
