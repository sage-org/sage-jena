package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;

/**
 * A reducer used to reconstruct AVG aggregation
 * @author Thomas Minier
 */
public class AvgReducer extends BinaryReducer {

    public AvgReducer(Var sumVariable, Var countVariable, ExecutionContext context) {
        super(sumVariable, countVariable, context);
    }

    @Override
    AggregationReducer firstReducer(Var sumVariable, ExecutionContext context) {
        return new CountSumReducer(sumVariable, context);
    }

    @Override
    AggregationReducer secondReducer(Var countVariable, ExecutionContext context) {
        return new CountSumReducer(countVariable, context);
    }

    @Override
    NodeValue combineBoth(NodeValue sum, NodeValue count) {
        return XSDFuncOp.numDivide(sum, count);
    }
}
