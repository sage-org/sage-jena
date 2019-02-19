package org.gdd.sage.engine.iterators.optional;

import com.google.common.collect.Sets;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;

/**
 * Implements an OptJoin, an optimized algorithm for OPTIONAL processing by a Sage Smart client.
 * see details in "SaGe: Web Preemption for Public SPARQL Query Services" in Proceedings of the 2019 World Wide Web Conference (WWW'19)
 * @author Thomas Minier
 */
public class OptJoin extends QueryIteratorBase {
    private QueryIterator source;
    private Set<Var> leftVariables;
    private Set<Var> joinVariables;
    private Deque<Binding> toServe;
    private Deque<Binding> unionView;
    private Deque<Binding> unmatchedResults;
    private boolean shouldBuildResults;

    /**
     * Constructor, for an OptJoin(P_1 OPT P_2)
     * @param source - Source iterator, which evaluates (P_1 JOIN P_2) UNION P_1
     * @param leftVariables - SPARQL variables of P_1
     * @param joinVariables - SPARQL variables of (P_1 JOIN P_2)
     */
    public OptJoin(QueryIterator source, Set<Var> leftVariables, Set<Var> joinVariables) {
        this.source = source;
        this.leftVariables = leftVariables;
        this.joinVariables = joinVariables;
        toServe = new LinkedList<>();
        unionView = new LinkedList<>();
        unmatchedResults = new LinkedList<>();
        shouldBuildResults = true;
    }

    /**
     * Perform a projection over a set of mappings, according to the set of variables of P_1
     * @param original - Set of mappings to transform
     * @return Projected set of mappings
     */
    private Binding projection(Binding original) {
        BindingHashMap bindings = new BindingHashMap();
        for(Var variable: leftVariables) {
            if (original.contains(variable)) {
                bindings.add(variable, original.get(variable));
            }
        }
        return bindings;
    }

    /**
     * Review internal buffers to build additional optional results,
     * by computing P_1 - (P_1 JOIN P_2)
     */
    private void buildOptionalResults() {
        for(Binding binding: unionView) {
            unmatchedResults.remove(projection(binding));
        }
    }

    /**
     * Pull bindings from the source iterator and insert it into the relevant materialized view
     */
    private void pullFromSource() {
        Binding binding = source.next();
        Set<Var> bindingVariables = Sets.newHashSet(binding.vars());
        if (Sets.difference(joinVariables, bindingVariables).isEmpty()) {
            unionView.add(new BindingHashMap(binding));
            toServe.add(new BindingHashMap(binding));
        } else {
            unmatchedResults.add(new BindingHashMap(binding));
        }
    }

    @Override
    protected boolean hasNextBinding() {
        if (source.hasNext()) {
            // try to produce a set of mappings from the source iterator
            while(source.hasNext() && toServe.isEmpty()) {
                pullFromSource();
            }
        }
        if (!toServe.isEmpty()) {
            return true;
        } else if (shouldBuildResults) {
            buildOptionalResults();
            shouldBuildResults = false;
        }
        return !unmatchedResults.isEmpty();
    }

    @Override
    protected Binding moveToNextBinding() {
        if (!toServe.isEmpty()) {
            return toServe.pop();
        } else if (shouldBuildResults) {
            buildOptionalResults();
            shouldBuildResults = false;
        }
        return unmatchedResults.pop();
    }

    @Override
    protected void closeIterator() {
        source.close();
        unionView.clear();
        unmatchedResults.clear();
    }

    @Override
    protected void requestCancel() {
        source.cancel();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.write("OptJoin(");
        source.output(out, sCxt);
        out.write(")");
    }
}
