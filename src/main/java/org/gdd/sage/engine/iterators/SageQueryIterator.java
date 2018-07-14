package org.gdd.sage.engine.iterators;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

/**
 * Base class used to implements Iterators that evaluate queries using a Sage server
 * @author Thomas Minier
 */
public abstract class SageQueryIterator extends QueryIteratorBase {
    protected SageRemoteClient client;
    protected Optional<String> nextLink;
    protected Deque<Binding> bindingsBuffer;
    protected boolean hasNextPage = false;
    protected Logger logger;
    private boolean warmup = true;

    /**
     * This method will be called each time the iterator needs to send another query to the server,
     * with an optional "next" link
     * @param nextLink Optional next link, used to resume query execution
     * @return Query execution results
     */
    protected abstract QueryResults query (Optional<String> nextLink);

    public SageQueryIterator(SageRemoteClient client) {
        this.client = client;
        this.nextLink = Optional.empty();
        this.bindingsBuffer = new ArrayDeque<>();
        logger = ARQ.getExecLogger();
    }

    protected void fillBindingsBuffer () {
        QueryResults queryResults = query(nextLink);
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
        if (bindingsBuffer.isEmpty()) {
            fillBindingsBuffer();
        }
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

    }
}
