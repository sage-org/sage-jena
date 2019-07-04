package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprLib;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * A base class for implementing aggregation reducers that operate on a single variable
 * @author Thomas Minier
 */
public abstract class UnaryReducer implements AggregationReducer {
    private Var variable;
    private Expr expr;
    private NodeValue currentValue;
    private ExecutionContext context;

    public UnaryReducer(Var variable, ExecutionContext context) {
        this.variable = variable;
        this.context = context;
        this.expr = new ExprVar(variable);
        this.currentValue = bottom();
    }

    /**
     * Get the initial value for the reducer
     * @return
     */
    abstract NodeValue bottom();

    /**
     * Merge two values into a new one.
     * This operation must be commutative, associative and idempotent.
     * @param x - Left value
     * @param y - Right value
     * @return Merged value
     */
    abstract NodeValue merge(NodeValue x, NodeValue y);

    /**
     * Reduce the value into the final aggregation result
     * @param v - Value to reduce
     * @return Final aggregation result
     */
    abstract NodeValue reduce(NodeValue v);

    @Override
    public void accumulate(Binding bindings) {
        if (bindings.contains(variable)) {
            // extract value
            NodeValue value = ExprLib.evalOrNull(expr, bindings, context);
            if (value != null) {
                currentValue = merge(currentValue, value);
            }
        }
    }

    @Override
    public NodeValue getFinalValue() {
        return reduce(currentValue);
    }

}
