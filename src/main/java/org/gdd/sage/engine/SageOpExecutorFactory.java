package org.gdd.sage.engine;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.StageBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Factory used to create a {@link SageOpExecutor}
 * @author Thomas Minier
 */
public class SageOpExecutorFactory implements OpExecutorFactory {
    private final ExecutorService threadPool;

    public SageOpExecutorFactory() {
        // TODO replace with a fixed size thread pool???
        threadPool = Executors.newCachedThreadPool();
    }

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        StageBuilder.setGenerator(execCxt.getContext(), SageStageGenerator.createDefault(threadPool));
        return new SageOpExecutor(threadPool, execCxt);
    }
}
