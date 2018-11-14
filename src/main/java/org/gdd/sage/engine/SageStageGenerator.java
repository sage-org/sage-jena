package org.gdd.sage.engine;

import com.google.common.collect.Lists;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.join.JoinKey;
import org.apache.jena.sparql.engine.join.QueryIterHashJoin;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.gdd.sage.engine.iterators.boundjoin.BoundJoinIterator;
import org.gdd.sage.engine.iterators.boundjoin.OptBoundJoinIterator;
import org.gdd.sage.http.data.QueryResults;
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
            boolean isOptional = execCxt.getContext().isTrue(SageSymbols.OPTIONAL_SYMBOL);
            if (input.isJoinIdentity() && !isOptional) {
                return sageGraph.basicGraphPatternFind(pattern);
            } else if (isOptional) {
                // use a bind join approach to evaluate Left join/Optionals
                return new OptBoundJoinIterator(input, sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE, execCxt);
            }
            // if we can download the right pattern in one call, use a hash join instead of a bound join
            /*QueryResults rightRes = sageGraph.getClient().query(pattern);
            if (!rightRes.hasNext()) {
                QueryIterator rightIter = new QueryIterPlainWrapper(rightRes.getBindings().iterator());
                return QueryIterHashJoin.create(input, rightIter, execCxt);
            }*/
            return new BoundJoinIterator(input, sageGraph.getClient(), pattern, BIND_JOIN_BUCKET_SIZE, execCxt);
        }
        return above.execute(pattern, input, execCxt);
    }
}
