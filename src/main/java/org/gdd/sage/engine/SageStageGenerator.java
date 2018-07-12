package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.join.Join;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.util.Symbol;
import org.gdd.sage.engine.iterators.BoundJoinIterator;
import org.gdd.sage.engine.iterators.OptionalBoundJoinIterator;
import org.gdd.sage.engine.iterators.SageBGPIterator;
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

        // This stage generator only support evaluation of a Sage Graph
        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;
            // detect if we need to perform a join or a left join
            boolean isOptional = execCxt.getContext().isTrue(Symbol.create("optional"));
            if (input.isJoinIdentity()) {
                return sageGraph.basicGraphPatternFind(pattern);
            } else if (isOptional) {
                // use a bind join approach to evaluate Left join/Optionals
                return new OptionalBoundJoinIterator(input, sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE);
            }
            /*SageBGPIterator bgpIt = (SageBGPIterator) sageGraph.basicGraphPatternFind(pattern);
            // if the BGP can be downloaded in one HTTP request, then use a hash join to save data transfer
            if (!bgpIt.getHasNextPage()) {
                return Join.hashJoin(input, bgpIt, execCxt);
            }*/
            // otherwise, use a Bind Join as default strategy
            return new BoundJoinIterator(input, sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE);
        }
        return above.execute(pattern, input, execCxt);
    }
}
