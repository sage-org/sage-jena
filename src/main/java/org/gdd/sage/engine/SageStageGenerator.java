package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.gdd.sage.core.analyzer.FilterRegistry;
import org.gdd.sage.engine.iterators.boundjoin.BoundJoinIterator;
import org.gdd.sage.model.SageGraph;

/**
 * Provides a custom StageGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {

    private StageGenerator above;
    private static final int BIND_JOIN_BUCKET_SIZE = 15;

    private SageStageGenerator(StageGenerator above) {
        this.above = above;
    }

    /**
     * Factory method used to create a SageStageGenerator with the default ARQ context
     * @return A SageStageGenerator configured with the default ARQ context
     */
    public static SageStageGenerator createDefault() {
        StageGenerator orig = ARQ.getContext().get(ARQ.stageGenerator);
        return new SageStageGenerator(orig);
    }

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph();

        // gather relevant filters to attach with the BGP evaluation
        /*Set<String> variables = SageUtils.getVariables(pattern).stream().map(Node_Variable::toString).collect(Collectors.toSet());
        String relevantFilters = filters.getFormattedFilters(variables);*/

        // This stage generator only support evaluation of a Sage Graph
        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;

            // no input bindings => simply evaluate the BGP
            if (input.isJoinIdentity()) {
                // TODO handle filters
                return sageGraph.basicGraphPatternFind(pattern);
            }

            // if we can download the right pattern in one call, use a hash join instead of a bound join
            /*QueryResults rightRes = sageGraph.getClient().query(pattern);
            if (!rightRes.hasNext()) {
                QueryIterator rightIter = new QueryIterPlainWrapper(rightRes.getBindings().iterator());
                return QueryIterHashJoin.create(input, rightIter, execCxt);
            }*/
            // otherwise, use a bind join
            return new BoundJoinIterator(input, sageGraph.getGraphURI(), sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE, execCxt);
        }

        // delegate execution of the unsupported Graph to the StageGenerator above
        return above.execute(pattern, input, execCxt);
    }
}
