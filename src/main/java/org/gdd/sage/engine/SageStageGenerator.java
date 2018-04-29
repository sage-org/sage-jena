package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.join.QueryIterHashJoin;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.gdd.sage.model.SageGraph;

/**
 * Provides a custom SateGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {

    private StageGenerator above;

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
            if (input instanceof QueryIterRoot) {
                return sageGraph.evaluateBGP(pattern);
            }
            SageBGPIterator bgpIt = (SageBGPIterator) sageGraph.evaluateBGP(pattern);
            // if the BGP can be downloaded in one HTTP request, then use a hash join to save data transfer
            if (!bgpIt.getHasNextPage()) {
                return QueryIterHashJoin.create(input, bgpIt, execCxt);
            }
            return new SageBGPJoinIterator(input, pattern, sageGraph, execCxt);
        }
        return above.execute(pattern, input, execCxt);
    }
}
