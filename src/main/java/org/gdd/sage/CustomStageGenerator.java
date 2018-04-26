package org.gdd.sage;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;

public class CustomStageGenerator implements StageGenerator {

    private String serverURL;

    public CustomStageGenerator(String serverURL) {
        this.serverURL = serverURL;
    }

    @Override
    public QueryIterator execute(BasicPattern pattern,
                                 QueryIterator input,
                                 ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph() ;
        System.out.println(input.nextBinding());

        return new QueryIterNullIterator(execCxt);
    }
}
