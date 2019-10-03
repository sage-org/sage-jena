package org.gdd.sage.engine.iterators.agg;

import org.apache.jena.atlas.iterator.IteratorDelayedInitialization;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.binding.BindingInputStream;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.gdd.sage.engine.reducers.AggregationReducer;
import org.gdd.sage.engine.reducers.CountSumReducer;
import org.gdd.sage.engine.reducers.GroupByReducer;
import org.gdd.sage.engine.reducers.Reducer;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.results.QueryResults;
import org.gdd.sage.model.SageGraph;

import java.util.*;

public class SageGroupByIterator extends QueryIterPlainWrapper {

    public SageGroupByIterator(SageGraph graph, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions, ExecutionContext exCxt) {
        super(compute(graph, bgp, variables, aggregations, extensions, exCxt));
    }

    public static Binding genKey(List<Var> variables, Binding bindings) {
        BindingHashMap b = new BindingHashMap();
        for(Var v: variables) {
            if (bindings.contains(v)) {
                b.add(v, bindings.get(v));
            } else {
                return null;
            }
        }
        return b;
    }

    private static Iterator<Binding> compute(SageGraph graph, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions, ExecutionContext exCxt) {
        return new IteratorDelayedInitialization<Binding>() {

            @Override
            protected Iterator<Binding> initializeIterator() {

                // create reducer to gather results
                // TODO choose a GroupByReducer for unsupported query shapes
                //Reducer reducer = new GroupByReducer();
                ReducerFactory factory = new ReducerFactory(aggregations, extensions, exCxt);


                // gather all query solutions
                QueryResults results;
                Map<Binding, List<Binding>> solutions = new HashMap<>();
                boolean hasNext = true;
                Optional<String> nextLink = Optional.empty();
                while (hasNext) {
                    results = graph.getClient().queryGroupBy(graph.getGraphURI(), bgp, variables, aggregations, extensions, nextLink);
                    // regroup all bindings by key
                    for(Binding b: results.getBindings()) {
                        Binding key = genKey(variables, b);
                        if (key != null) {
                            if (!solutions.containsKey(key)) {
                                solutions.put(key, new LinkedList<>());
                            }
                            solutions.get(key).add(b);
                        }
                    }
                    /*results.getSolutionGroups().forEach(solutionGroup -> {
                        reducer.accumulate(solutionGroup);
                        cpt[0] += solutionGroup.groupSize();
                    });*/
                    nextLink = results.getNext();
                    hasNext = results.hasNext();
                }

                // produce final results from each group
                return solutions.entrySet().parallelStream().map(entry -> {
                    Binding res = new BindingHashMap();
                    Binding key = entry.getKey();

                    // add grouping keys as results
                    ((BindingHashMap) res).addAll(key);

                    // build reducers for this group
                    Map<Var, AggregationReducer> reducers = factory.build();

                    // accumulate values into the reducers
                    for(Binding b: entry.getValue()) {
                        reducers.forEach((v, reducer) -> reducer.accumulate(b));
                    }

                    // build final results
                    for(Map.Entry<Var, AggregationReducer> reducer: reducers.entrySet()) {
                        Node value = reducer.getValue().getFinalValue().asNode();
                        ((BindingHashMap) res).add(reducer.getKey(), value);
                    }
                    return res;
                }).iterator();


                // TODO change that to support others operators (same as above)
                // apply aggregations on each group
                /*return reducer.getGroups().parallelStream().map(solutionGroup -> {
                    Binding bindings = new BindingHashMap();
                    // add aggregation keys in the bindings
                    solutionGroup.forEachKey((var, node) -> ((BindingHashMap) bindings).add(var, node));
                    // apply each accumulator
                    for(ExprAggregator agg: aggregations) {
                        Var bindsTo = agg.getVar();
                        Accumulator accumulator = agg.getAggregator().createAccumulator();
                        solutionGroup.forEachBindings(binding -> accumulator.accumulate(binding, exCxt));
                        ((BindingHashMap) bindings).add(bindsTo, accumulator.getValue().asNode());
                    }
                    return bindings;
                }).iterator();*/
            }
        };
    }
}
