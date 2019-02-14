package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.QC;
import org.gdd.sage.engine.iterators.OptionalIterator;

/**
 * An OpExecutor that streams intermediate results for OpGraph and OpService operators,
 * and use a Bind join approach to evaluate Optionals.
 *
 * The default ARQ implementation perform join "binding per binding" using QueryIterSingleton to wrap
 * intermediate results. While this approach performs great when using Nested Loop Joins, joins
 * algorithms like Symmetric hash join, that require streaming inputs, cannot be applied.
 * @author Thomas Minier
 */
public class StreamingOpExecutor extends OpExecutor {

    StreamingOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    protected QueryIterator exec(Op op, QueryIterator input) {
        return super.exec(op, input);
    }

    /**
     * Exxecute a Left Join/Optional with a pure pipeline logic
     * @param left - Left operator
     * @param right - Right operator
     * @param input - Solution bindings input
     * @return A QueryIterator that execute the left join
     */
    private QueryIterator executeOptional(Op left, Op right, QueryIterator input) {
        QueryIterator leftIter = QC.execute(left, input, execCxt);
        return OptionalIterator.create(leftIter, right, execCxt);
    }

    @Override
    protected QueryIterator execute(OpService opService, QueryIterator input) {
        DatasetGraph dataset = execCxt.getDataset();
        Node graphNode = opService.getService();
        if ((!graphNode.isURI()) && (!graphNode.isBlank())) {
            throw new ARQInternalErrorException("Unexpected SERVICE node (not an URI) when evaluating SERVICE clause.\n" + graphNode);
        } else if (Quad.isDefaultGraph(graphNode)) {
            return exec(opService.getSubOp(), input);
        } else if (!dataset.containsGraph(graphNode)) {
            throw new ARQInternalErrorException("Dataset does not contains the named graph <" + graphNode + ">");
        }
        Graph currentGraph = dataset.getGraph(graphNode);
        ExecutionContext graphContext = new ExecutionContext(execCxt, currentGraph);
        return QC.execute(opService.getSubOp(), input, graphContext);
    }

    @Override
    protected QueryIterator execute(OpConditional opCondition, QueryIterator input) {
        return executeOptional(opCondition.getLeft(), opCondition.getRight(), input);
    }

    @Override
    protected QueryIterator execute(OpLeftJoin opLeftJoin, QueryIterator input) {
        return executeOptional(opLeftJoin.getLeft(), opLeftJoin.getRight(), input);
    }
}
