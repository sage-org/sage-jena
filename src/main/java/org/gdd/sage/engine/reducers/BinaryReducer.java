package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * A base class for implementing aggregation reducers that combine two unary reducers
 * @author Thomas Minier
 */
public abstract class BinaryReducer implements AggregationReducer {

    private AggregationReducer firstRed;
    private AggregationReducer secondRed;
    private ExecutionContext context;

    public BinaryReducer(Var firstVariable, Var secondVariable, ExecutionContext context) {
        firstRed = firstReducer(firstVariable, context);
        secondRed = secondReducer(secondVariable, context);
        this.context = context;
    }

    /**
     * Build the first reducer
     * @param variable - Variable on which the first reducer operates
     * @param context - execution context
     * @return The first reducer
     */
    abstract AggregationReducer firstReducer(Var variable, ExecutionContext context);

    /**
     * Build the second reducer
     * @param variable - Variable on which the second reducer operates
     * @param context - execution context
     * @return The second reducer
     */
    abstract AggregationReducer secondReducer(Var variable, ExecutionContext context);

    /**
     * Combine the final values of both reducers
     * @param first - Final value of the first reducer
     * @param second - Final value of the second reducer
     * @return Combined final value
     */
    abstract NodeValue combineBoth(NodeValue first, NodeValue second);

    @Override
    public void accumulate(Binding bindings) {
        firstRed.accumulate(bindings);
        secondRed.accumulate(bindings);
    }

    @Override
    public NodeValue getFinalValue() {
        return combineBoth(firstRed.getFinalValue(), secondRed.getFinalValue());
    }
}
