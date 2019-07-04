package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;

/**
 * A reducer used to reconstruct COUNT or SUM aggregation
 * @author Thomas Minier
 */
public class CountSumReducer extends UnaryReducer {

    public CountSumReducer(Var variable, ExecutionContext context) {
        super(variable, context);
    }

    @Override
    NodeValue bottom() {
        return NodeValue.makeInteger(0);
    }

    @Override
    NodeValue merge(NodeValue x, NodeValue y) {
        return XSDFuncOp.numAdd(x, y);
    }

    @Override
    NodeValue reduce(NodeValue v) {
        return v;
    }
}
