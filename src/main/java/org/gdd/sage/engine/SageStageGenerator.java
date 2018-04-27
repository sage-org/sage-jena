package org.gdd.sage.engine;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.gdd.sage.http.SageRemoteClient;

/**
 * Provides a custom SateGenerator for evaluating SPARQL queries against a SaGe server
 * @author Thomas Minier
 */
public class SageStageGenerator implements StageGenerator {

    private String serverURL;
    private SageRemoteClient httpClient;

    /**
     * Constructor
     * @param serverURL - URL of the SaGe server to use
     */
    public SageStageGenerator(String serverURL) {
        this.serverURL = serverURL;
        this.httpClient = new SageRemoteClient(this.serverURL);
    }

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        return new SageBGPIterator(httpClient, pattern);
        /*Graph g = execCxt.getActiveGraph() ;
        System.out.println(input.nextBinding());
        return new QueryIterNullIterator(execCxt);*/
    }
}
