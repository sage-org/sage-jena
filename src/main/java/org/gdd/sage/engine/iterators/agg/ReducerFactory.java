package org.gdd.sage.engine.iterators.agg;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.tdb.store.Hash;
import org.gdd.sage.engine.reducers.AggregationReducer;
import org.gdd.sage.engine.reducers.CountSumReducer;
import org.gdd.sage.engine.reducers.MaxReducer;
import org.gdd.sage.engine.reducers.MinReducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class ReducerFactory {

    private Map<Var, Supplier<AggregationReducer>> suppliers;
    private ExecutionContext context;

    public ReducerFactory(List<ExprAggregator> aggregations, VarExprList extensions, ExecutionContext context) {
        suppliers = new HashMap<>();
        this.context = context;

        for(ExprAggregator exprAgg: aggregations) {
            // search for the correct variable
            extensions.getExprs().forEach((var, expr) -> {
                if (exprAgg.getVar().equals(expr.asVar())) {
                    suppliers.put(var, buildSupplier(var, exprAgg));
                }
            });
        }

    }

    private Supplier<AggregationReducer> buildSupplier(Var variable, ExprAggregator exprAggregator) {
        switch (exprAggregator.getAggregator().getName()) {
            case "COUNT": {
                return () -> new CountSumReducer(variable, context);
            }
            case "SUM": {
                return () -> new CountSumReducer(variable, context);
            }
            case "MIN": {
                return () -> new MinReducer(variable, context);
            }
            case "MAX": {
                return () -> new MaxReducer(variable, context);
            }
            default: {
                return null;
            }
        }
    }

    public Map<Var, AggregationReducer> build() {
        Map<Var, AggregationReducer> res = new HashMap<>();
        suppliers.forEach((var, supplier) -> res.put(var, supplier.get()));
        return res;
    }
}
