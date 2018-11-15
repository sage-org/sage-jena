package org.gdd.sage.engine;

import com.google.common.collect.Lists;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.join.JoinKey;
import org.apache.jena.sparql.engine.join.QueryIterHashJoin;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.gdd.sage.core.SageUtils;
import org.gdd.sage.core.analyzer.FilterRegistry;
import org.gdd.sage.engine.iterators.boundjoin.BoundJoinIterator;
import org.gdd.sage.engine.iterators.boundjoin.OptBoundJoinIterator;
import org.gdd.sage.http.data.QueryResults;
import org.gdd.sage.model.SageGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides a custom StageGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {

    private StageGenerator above;
    private FilterRegistry filters;
    private static final int BIND_JOIN_BUCKET_SIZE = 15;

    private SageStageGenerator(StageGenerator above) {
        this.above = above;
        this.filters = new FilterRegistry();
    }

    private SageStageGenerator(StageGenerator above, FilterRegistry filters) {
        this.above = above;
        this.filters = filters;
    }

    /**
     * Factory method used to create a SageStageGenerator with the default ARQ context
     * @return A SageStageGenerator configured with the default ARQ context
     */
    public static SageStageGenerator createDefault() {
        StageGenerator orig = ARQ.getContext().get(ARQ.stageGenerator);
        return new SageStageGenerator(orig);
    }

    /**
     * Factory method used to create a SageStageGenerator with the default ARQ context
     * and a set of Filters expressions collected beforehand.
     * @return A SageStageGenerator configured with the default ARQ context
     */
    public static SageStageGenerator createWithFilters(FilterRegistry filters) {
        StageGenerator orig = ARQ.getContext().get(ARQ.stageGenerator);
        return new SageStageGenerator(orig, filters);
    }

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph();

        // gather relevant filters to attach with the BGP evaluation
        Set<String> variables = SageUtils.getVariables(pattern).stream().map(Node_Variable::toString).collect(Collectors.toSet());
        String relevantFilters = filters.getFormattedFilters(variables);

        // This stage generator only support evaluation of a Sage Graph
        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;
            // detect if we need to perform a join or a left join
            boolean isOptional = execCxt.getContext().isTrue(SageSymbols.OPTIONAL_SYMBOL);

            // no input bindings => simply evaluate the BGP
            if (input.isJoinIdentity() && !isOptional) {
                return sageGraph.basicGraphPatternFind(pattern, relevantFilters);
            } else if (isOptional) {
                // use a bind join approach to evaluate Left join/Optionals
                return new OptBoundJoinIterator(input, sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE, execCxt);
            }

            // if we can download the right pattern in one call, use a hash join instead of a bound join
            QueryResults rightRes = sageGraph.getClient().query(pattern);
            if (!rightRes.hasNext()) {
                QueryIterator rightIter = new QueryIterPlainWrapper(rightRes.getBindings().iterator());
                return QueryIterHashJoin.create(input, rightIter, execCxt);
            }
            return new BoundJoinIterator(input, sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE, execCxt);
        }

        // delegate execution of the unsupported Graph to the StageGenerator above
        return above.execute(pattern, input, execCxt);
    }
}
