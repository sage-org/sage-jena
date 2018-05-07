package org.gdd.sage.engine;

import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.util.Context;

/**
 * Used to configure the default Sage Execution context, by registering a {@link StreamingOpExecutor} as OpExecutor
 * and a {@link SageStageGenerator} as StageGenerator
 */
public class SageExecutionContext {
    /**
     * Configure the default Sage Execution context
     * @param execContext - Usually get by calling ARQ.getContext()
     */
    public static void configureDefault(Context execContext) {
        OpExecutorFactory opFactory = new StreamingOpExecutorFactory();
        QC.setFactory(execContext, opFactory);
        StageBuilder.setGenerator(execContext, SageStageGenerator.createDefault());
    }
}
