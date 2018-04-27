package org.gdd.sage.engine;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.gdd.sage.http.QueryResults;
import org.gdd.sage.http.SageRemoteClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Evaluate a Basic Graph Pattern (BGP) using a SaGe server using the Iterator pattern.
 * It fetches results in a lazy manner, i.e, a new HTTP request is issued only after all results
 * fetched by the previous one has been completely consumed.
 * @author Thomas Minier
 */
public class SageBGPIterator extends QueryIteratorBase {

    private SageRemoteClient client;
    private BasicPattern bgp;
    private String nextLink;
    private List<Binding> bindingsBuffer;
    private boolean isDone;

    /**
     * Constructor
     * @param client - HTTP client used to query the SaGe server
     * @param bgp - Basic Graph pattern to evaluate
     */
    public SageBGPIterator(SageRemoteClient client, BasicPattern bgp) {
        this.client = client;
        this.bgp = bgp;
        this.nextLink = null;
        this.bindingsBuffer = new ArrayList<>();
        this.isDone = false;
    }

    /**
     * Append solution bindings in the Iterator buffer
     * @param bindings - Solution bindings to append
     */
    public void append(List<Binding> bindings) {
        bindingsBuffer.addAll(bindings);
    }

    @Override
    protected boolean hasNextBinding() {
        return !bindingsBuffer.isEmpty() || !isDone;
    }

    @Override
    protected Binding moveToNextBinding() {
        if (this.hasNextBinding()) {
            // pull from internal buffer is possible, otherwise fetch more bindings from server
            if (!bindingsBuffer.isEmpty()) {
                Binding binding = bindingsBuffer.get(0);
                bindingsBuffer.remove(0);
                return binding;
            }
            try {
                QueryResults queryResults = client.query(bgp, nextLink);
                bindingsBuffer.addAll(queryResults.bindings);
                nextLink = queryResults.next;
                if (!queryResults.hasNext()) {
                    isDone = true;
                }
                return this.moveToNextBinding();
            } catch (IOException e) {
                isDone = true;
            }
        }
        return null;
    }

    @Override
    protected void closeIterator() {
        isDone = true;
        bindingsBuffer.clear();
    }

    @Override
    protected void requestCancel() {

    }

    @Override
    public void output(IndentedWriter indentedWriter, SerializationContext serializationContext) {

    }
}
