package org.gdd.sage.engine;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;

/**
 * Factory used to create a StreamingOpExecutor
 * @author Thomas Minier
 */
public class StreamingOpExecutorFactory implements OpExecutorFactory {
    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new StreamingOpExecutor(execCxt);
    }
}
