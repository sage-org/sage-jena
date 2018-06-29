package org.gdd.sage.engine.iterators;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Evaluate an Optional clause using a Bind join approach
 * @author Thomas Minier
 */
public class BindOptionalIterator extends SageBGPIterator {

    private QueryIterator source;
    private List<Binding> tempBuffer;
    private List<BasicPattern> bgpBuffer;
    private int bufferSize;

    /**
     * Constructor
     * @param source - Input for the left join
     * @param client - HTTP client used to query the SaGe server
     * @param bgp    - Basic Graph pattern to left join with
     * @param bufferSize - Size of the bind join buffer (15 is the "default" admitted value)
     */
    public BindOptionalIterator(QueryIterator source, SageRemoteClient client, BasicPattern bgp, int bufferSize) {
        super(client, bgp);
        this.source = source;
        this.tempBuffer = new ArrayList<>();
        this.bgpBuffer = new ArrayList<>();
        this.bufferSize = bufferSize;
    }

    @Override
    protected boolean hasNextBinding() {
        return source.hasNext() || super.hasNextBinding();
    }

    @Override
    protected void fillBindingsBuffer() {
        // if no next page, try to read from source to build a buffer of bounded BGps
        if (!nextLink.isPresent()) {
            tempBuffer.clear();
            bgpBuffer.clear();
            while (source.hasNext() && this.bgpBuffer.size() < bufferSize) {
                Binding b = source.nextBinding();
                BasicPattern boundedBGP = new BasicPattern();
                for (Triple t: bgp) {
                    boundedBGP.add(Substitute.substitute(t, b));
                }
                bgpBuffer.add(boundedBGP);
                tempBuffer.add(b);
            }
        }

        if (!bgpBuffer.isEmpty()) {
            // send union query to sage server
            QueryResults queryResults = client.query(bgpBuffer, nextLink);
            List<Binding> solutions = queryResults.getBindings();
            if (queryResults.hasError()) {
                // an error has occurred, report it
                hasNextPage = false;
                logger.error(queryResults.getError());
            } else if (solutions.isEmpty()) {
                // optional part: return buffer of input bindings
                bindingsBuffer.addAll(tempBuffer);
                hasNextPage = false;
                nextLink = Optional.empty();
            } else {
                // found results!
                bindingsBuffer.addAll(solutions);
                nextLink = queryResults.getNext();
                hasNextPage = queryResults.hasNext();
            }
        }
    }
}
