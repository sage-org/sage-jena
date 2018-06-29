package org.gdd.sage.engine.iterators;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * Evaluate a Basic Graph Pattern (BGP) using a SaGe server using the Iterator pattern.
 * It fetches results in a lazy manner, i.e, a new HTTP request is issued only after all results
 * fetched by the previous one has been completely consumed.
 * @author Thomas Minier
 */
public class SageBGPIterator extends QueryIteratorBase {

    protected SageRemoteClient client;
    protected BasicPattern bgp;
    protected Optional<String> nextLink;
    protected Deque<Binding> bindingsBuffer;
    protected boolean hasNextPage = false;
    protected Logger logger;
    private boolean warmup = true;

    /**
     * Constructor
     * @param client - HTTP client used to query the SaGe server
     * @param bgp - Basic Graph pattern to evaluate
     */
    public SageBGPIterator(SageRemoteClient client, BasicPattern bgp) {
        this.client = client;
        this.bgp = bgp;
        this.nextLink = Optional.empty();
        this.bindingsBuffer = new ArrayDeque<>();

        logger = ARQ.getExecLogger();
    }

    /**
     * Append solution bindings in the Iterator buffer
     * @param bindings - Solution bindings to append
     */
    public void append(List<Binding> bindings) {
        bindingsBuffer.addAll(bindings);
    }

    /**
     * Return True if the Iterator has all bindings have not been retrieved from the server
     * @return
     */
    public boolean getHasNextPage() {
        return this.hasNextPage;
    }

    protected void fillBindingsBuffer () {
        QueryResults queryResults = client.query(bgp, nextLink);
        if (queryResults.hasError()) {
            hasNextPage = false;
            logger.error(queryResults.getError());
        } else {
            bindingsBuffer.addAll(queryResults.getBindings());
            nextLink = queryResults.getNext();
            hasNextPage = queryResults.hasNext();
        }
    }

    @Override
    protected boolean hasNextBinding() {
        if (warmup) {
            this.fillBindingsBuffer();
            warmup = false;
        }
        return (!bindingsBuffer.isEmpty()) || hasNextPage;
    }

    @Override
    protected Binding moveToNextBinding() {
        // pull from internal buffer is possible, otherwise fetch more bindings from server
        if (!bindingsBuffer.isEmpty()) {
            return bindingsBuffer.pollFirst();
        }
        fillBindingsBuffer();
        return bindingsBuffer.pollFirst();
    }

    @Override
    protected void closeIterator() {
        hasNextPage = false;
        bindingsBuffer.clear();
    }

    @Override
    protected void requestCancel() {

    }

    @Override
    public void output(IndentedWriter indentedWriter, SerializationContext serializationContext) {
        indentedWriter.printf("SageBGPIterator { %s } @<%s>", bgp, client.getServerURL());
    }
}
