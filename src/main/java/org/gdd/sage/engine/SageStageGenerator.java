package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.model.SageGraph;

/**
 * Provides a custom SateGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph();
        if (g instanceof SageGraph) {
            SageGraph sageGraph = (SageGraph) g;
            if (input instanceof QueryIterRoot) {
                return sageGraph.evaluateBGP(pattern);
            }
            return new SageBGPJoinIterator(input, pattern, sageGraph, execCxt);
        }
        return null;
    }
}
