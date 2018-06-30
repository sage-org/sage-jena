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

/**
 * An iterator which evaluates a Bind Join between an input iterator and a BGP
 * @author Thomas Minier
 */
public class BindJoinIterator extends SageBGPIterator {

    private QueryIterator source;
    protected List<Binding> tempBuffer;
    protected List<BasicPattern> bgpBuffer;
    private int bufferSize;

    /**
     * Constructor
     * @param source - Input for the left join
     * @param client - HTTP client used to query the SaGe server
     * @param bgp    - Basic Graph pattern to left join with
     * @param bufferSize - Size of the bind join buffer (15 is the "default" admitted value)
     */
    public BindJoinIterator(QueryIterator source, SageRemoteClient client, BasicPattern bgp, int bufferSize) {
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

    /**
     * Do something with bond join query results
     * @param results - Query results fetched from the Sage server
     */
    protected void reviewResults(QueryResults results) {
        List<Binding> solutions = results.getBindings();
        if (!solutions.isEmpty()) {
            bindingsBuffer.addAll(solutions);
            nextLink = results.getNext();
            hasNextPage = results.hasNext();
        }
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
            if (queryResults.hasError()) {
                // an error has occurred, report it
                hasNextPage = false;
                logger.error(queryResults.getError());
            } else {
                reviewResults(queryResults);
            }
        }
    }
}
