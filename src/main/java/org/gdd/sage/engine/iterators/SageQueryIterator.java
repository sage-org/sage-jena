package org.gdd.sage.engine.iterators;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Base class used to implements Iterators that evaluate queries using a Sage server
 * @author Thomas Minier
 */
public abstract class SageQueryIterator extends BufferedIterator {
    protected SageRemoteClient client;
    protected Optional<String> nextLink;
    protected boolean hasNextPage = false;
    protected Logger logger;

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
        logger = ARQ.getExecLogger();
    }

    @Override
    protected boolean canProduceBindings() {
        return hasNextPage;
    }

    @Override
    protected List<Binding> produceBindings() {
        return query(nextLink).getBindings();
    }

    @Override
    protected void closeIterator() {
        super.closeIterator();
        hasNextPage = false;
    }
}
