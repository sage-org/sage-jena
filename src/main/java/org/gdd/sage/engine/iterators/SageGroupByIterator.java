package org.gdd.sage.engine.iterators;

import org.apache.jena.atlas.iterator.IteratorDelayedInitialization;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.gdd.sage.engine.reducers.GroupByReducer;
import org.gdd.sage.engine.reducers.Reducer;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.results.QueryResults;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class SageGroupByIterator extends QueryIterPlainWrapper {

    public SageGroupByIterator(String graphURI, SageRemoteClient client, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregators, ExecutionContext exCxt) {
        super(compute(graphURI, client, bgp, variables, aggregators, exCxt));
    }

    private static Iterator<Binding> compute(String graphURI, SageRemoteClient client, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregators, ExecutionContext exCxt) {
        return new IteratorDelayedInitialization<Binding>() {

            @Override
            protected Iterator<Binding> initializeIterator() {

                // create reducer to gather results
                // TODO only supports plain GROUP BY, need change the reducer to support COUNT, DISTINCT, etc
                Reducer reducer = new GroupByReducer();

                // gather all query solutions
                QueryResults results;
                boolean hasNext = true;
                Optional<String> nextLink = Optional.empty();
                final int[] nbCalls = {0};
                final int[] cpt = {0};
                while (hasNext) {
                    nbCalls[0] += 1;
                    results = client.queryGroupBy(graphURI, bgp, variables, nextLink);
                    results.getSolutionGroups().forEach(solutionGroup -> {
                        reducer.accumulate(solutionGroup);
                        cpt[0] += solutionGroup.groupSize();
                    });
                    nextLink = results.getNext();
                    hasNext = results.hasNext();
                }

                System.out.println(cpt[0] + " bindings found in total");
                System.out.println(nbCalls[0] + " HTTP requests");

                // TODO change that to support others operators (same as above)
                // apply aggregators on each group
                return reducer.getGroups().parallelStream().map(solutionGroup -> {
                    Binding bindings = new BindingHashMap();
                    // add aggregation keys in the bindings
                    solutionGroup.forEachKey((var, node) -> ((BindingHashMap) bindings).add(var, node));
                    // apply each accumulator
                    for(ExprAggregator agg: aggregators) {
                        Var bindsTo = agg.getVar();
                        Accumulator accumulator = agg.getAggregator().createAccumulator();
                        solutionGroup.forEachBindings(binding -> accumulator.accumulate(binding, exCxt));
                        ((BindingHashMap) bindings).add(bindsTo, accumulator.getValue().asNode());
                    }
                    return bindings;
                }).iterator();
            }
        };
    }
}
