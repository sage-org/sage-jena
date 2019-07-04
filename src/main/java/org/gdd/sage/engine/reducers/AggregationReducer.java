package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * A reducer for reconstructing SPARQL aggregations
 * @author Thomas Minier
 */
public interface AggregationReducer {

    /**
     * Accumulate a new set of bindings into the reducer
     * @param bindings - Set of bindings to accumulate
     */
    void accumulate(Binding bindings);

    /**
     * Get the final aggregation result
     * @return Aggregation result
     */
    NodeValue getFinalValue();
}
