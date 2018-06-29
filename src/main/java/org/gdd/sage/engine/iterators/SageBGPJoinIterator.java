package org.gdd.sage.engine.iterators;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.gdd.sage.model.SageGraph;

/**
 * Perform a Nested Loop Join between a source of bindings and a Basic Graph patterns. BGPs are evaluated using a SageServer.
 * @author Thomas Minier
 */
public class SageBGPJoinIterator extends QueryIterRepeatApply {

    private BasicPattern bgp;
    private SageGraph graph;

    public SageBGPJoinIterator(QueryIterator source, BasicPattern bgp, SageGraph graph, ExecutionContext context) {
        super(source, context);
        this.bgp = bgp;
        this.graph = graph;
    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        BasicPattern boundBGP = new BasicPattern();
        for (Triple t: bgp) {
            boundBGP.add(Substitute.substitute(t, binding));
        }
        return graph.basicGraphPatternFind(boundBGP);
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.print("SageBGPJoinIterator");
        super.output(out, sCxt);
        out.printf("{ %s }", bgp);
    }
}
