package org.gdd.sage.engine;

import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;

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
        return exec(opGraph.getSubOp(), input);
    }
}
