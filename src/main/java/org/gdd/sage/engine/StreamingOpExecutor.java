package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.QC;

/**
 * An OpExecutor that streams intermediate results for OpGraph and OpService operators.
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

    /**
     * Execute an Operator with a given named Graph and input
     * @param graphNode - Node which identify the named graph
     * @param op - Operator to execute
     * @param input - Solution bindings input
     * @return A QueryIterator that execute the operator
     */
    private QueryIterator executeWithGraph(Node graphNode, Op op, QueryIterator input) {
        DatasetGraph dataset = execCxt.getDataset();
        if ((!graphNode.isURI()) && (!graphNode.isBlank())) {
            throw new ARQInternalErrorException("Unexpected Graph node (not an URI) when evaluating graph clause.\n" + graphNode);
        } else if (Quad.isDefaultGraph(graphNode)) {
            return exec(op, input);
        } else if (!dataset.containsGraph(graphNode)) {
            throw new ARQInternalErrorException("Dataset does not contains the named graph <" + graphNode + ">");
        }
        Graph currentGraph = dataset.getGraph(graphNode);
        ExecutionContext graphContext = new ExecutionContext(execCxt, currentGraph);
        return QC.execute(op, input, graphContext);
    }

    @Override
    protected QueryIterator execute(OpGraph opGraph, QueryIterator input) {
        return executeWithGraph(opGraph.getNode(), opGraph.getSubOp(), input);
    }

    @Override
    protected QueryIterator execute(OpService opService, QueryIterator input) {
        return executeWithGraph(opService.getService(), opService.getSubOp(), input);
    }
}
