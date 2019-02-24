package org.gdd.sage.engine;

import com.google.common.collect.Sets;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.gdd.sage.core.SageUtils;
import org.gdd.sage.engine.iterators.boundjoin.BoundJoinIterator;
import org.gdd.sage.engine.iterators.boundjoin.ParallelBoundJoinIterator;
import org.gdd.sage.model.SageGraph;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Provides a custom StageGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {
    private final ExecutorService threadPool;
    private StageGenerator above;
    private static final int BIND_JOIN_BUCKET_SIZE = 15;

    private SageStageGenerator(StageGenerator above, ExecutorService threadPool) {
        this.above = above;
        this.threadPool = threadPool;
    }

    /**
     * Factory method used to create a SageStageGenerator with the default ARQ context
     * @return A SageStageGenerator configured with the default ARQ context
     */
    public static SageStageGenerator createDefault(ExecutorService threadPool) {
        StageGenerator orig = ARQ.getContext().get(ARQ.stageGenerator);
        return new SageStageGenerator(orig, threadPool);
    }

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph();

        // This stage generator only support evaluation of a Sage Graph
        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;

            // no input bindings => simply evaluate the BGP
            if (input.isJoinIdentity()) {
                return sageGraph.basicGraphPatternFind(pattern);
            }

            // if we can download the right pattern in one call, use a hash join instead of a bound join
            /*QueryResults rightRes = sageGraph.getClient().query(pattern);
            if (!rightRes.hasNext()) {
                QueryIterator rightIter = new QueryIterPlainWrapper(rightRes.getBindings().iterator());
                return QueryIterHashJoin.create(input, rightIter, execCxt);
            }*/
            // otherwise, use a bind join
            return new ParallelBoundJoinIterator(input, sageGraph.getGraphURI(), sageGraph.getClient(), pattern, threadPool, BIND_JOIN_BUCKET_SIZE);
        }

        // delegate execution of the unsupported Graph to the StageGenerator above
        return above.execute(pattern, input, execCxt);
    }

    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt,
                                 ExprList filters) {
        Graph g = execCxt.getActiveGraph();

        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;

            // no input bindings => simply evaluate the BGP
            if (input.isJoinIdentity()) {
                // compute which filters can be packed with the BGP
                return sageGraph.basicGraphPatternFind(pattern, findRelevantFilters(filters, pattern));
            }

            // otherwise, use a bind join
            return new ParallelBoundJoinIterator(input, sageGraph.getGraphURI(), sageGraph.getClient(), pattern, threadPool, BIND_JOIN_BUCKET_SIZE);
        }
        return above.execute(pattern, input, execCxt);
    }

    /**
     * Find all filters that can be applied to a Basic Graph Pattern
     * @param filters - List of filters to analyze
     * @param bgp - Basic graph pattern
     * @return The list of all filters that can be applied to the Basic Graph Pattern
     */
    private List<Expr> findRelevantFilters(ExprList filters, BasicPattern bgp) {
        List<Expr> relevantFilters = new LinkedList<>();
        Set<Var> bgpVariables = SageUtils.getVariables(bgp);
        for(Expr filter: filters.getList()) {
            Set<Var> filterVariables = filter.getVarsMentioned();
            // test if filterVariables is a subset of bgpVariables, i.e., filterVariables - bgpVariables = empty set
            if(Sets.difference(filterVariables, bgpVariables).isEmpty()) {
                relevantFilters.add(filter);
            }
        }
        System.out.println(relevantFilters);
        return relevantFilters;
    }
}
