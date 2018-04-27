package org.gdd.sage.engine;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.model.SageGraph;

/**
 * Perform a Nested Loop Join between a source of bindings and a Basic Graph patterns.
 * BGP are evaluated using a SageServer.
 * @author Thomas Minier
 */
public class SageBGPJoinIterator extends QueryIteratorBase {

    private QueryIterator source;
    private BasicPattern bgp;
    private SageGraph graph;
    private QueryIterator currentIterator;
    private Binding currentSourceBinding;
    private ExecutionContext context;

    public SageBGPJoinIterator(QueryIterator source, BasicPattern bgp, SageGraph graph, ExecutionContext context) {
        this.source = source;
        this.bgp = bgp;
        this.graph = graph;
        this.context = context;
        currentSourceBinding = new BindingHashMap();
        currentIterator = this.buildInnerLoop();
    }

    private QueryIterator buildInnerLoop() {
        if (source.hasNext()) {
            currentSourceBinding = source.next();
            if (currentSourceBinding == null) {
                return new QueryIterNullIterator(context);
            }
            BasicPattern boundBGP = new BasicPattern();
            for (Triple t: bgp) {
                boundBGP.add(Substitute.substitute(t, currentSourceBinding));
            }
            return graph.evaluateBGP(boundBGP);
        }
        return new QueryIterNullIterator(context);
    }

    @Override
    protected boolean hasNextBinding() {
        return source.hasNext() || currentIterator.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        if (this.hasNextBinding()) {
            // read from the current QueryIterator first, or pull from the source to create a new QueryIterator
            while (!this.currentIterator.hasNext()) {
                currentIterator = buildInnerLoop();
            }
            Binding mu = currentIterator.next();
            if (mu != null) {
                BindingHashMap res = new BindingHashMap();
                res.addAll(currentSourceBinding);
                res.addAll(mu);
                return res;
            }
            return mu;
        }
        return null;
    }

    @Override
    protected void closeIterator() {
        source.close();
        currentIterator.close();
    }

    @Override
    protected void requestCancel() {

    }

    @Override
    public void output(IndentedWriter indentedWriter, SerializationContext serializationContext) {

    }
}
