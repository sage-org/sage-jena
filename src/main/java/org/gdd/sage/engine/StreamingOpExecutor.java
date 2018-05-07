package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.QC;

/**
 * An OpExecutor that streams intermediate results for OpGraph and OpConditional operators.
 *
 * The default ARQ implementation perform join "binding per binding" using QueryIterSingleton to wrap
 * intermediate results. WHile this approach performs great when using Nested Loop Joins, joins
 * algorithms like Symmetric hash join, that require streaming inputs, cannot be applied.
 * @author Thomas Minier
 */
public class StreamingOpExecutor extends OpExecutor {

    protected StreamingOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    protected QueryIterator execute(OpGraph opGraph, QueryIterator input) {
        Node graphNode = opGraph.getNode();
        DatasetGraph dataset = execCxt.getDataset();
        if ((!graphNode.isURI()) && (!graphNode.isBlank())) {
            throw new ARQInternalErrorException("Unexpected Graph node (not an URI) when evaluating graph clause.\n" + graphNode);
        } else if (Quad.isDefaultGraph(graphNode)) {
            return exec(opGraph.getSubOp(), input);
        } else if (!dataset.containsGraph(graphNode)) {
            throw new ARQInternalErrorException("Dataset does not contains the named graph <" + graphNode + ">");
        }
        // execute graph sub-operations using the named graph as the new execution context
        Graph currentGraph = dataset.getGraph(graphNode);
        ExecutionContext graphContext = new ExecutionContext(execCxt, currentGraph);
        return QC.execute(opGraph.getSubOp(), input, graphContext);
    }
}
