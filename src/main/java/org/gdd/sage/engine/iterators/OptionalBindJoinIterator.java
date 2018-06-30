package org.gdd.sage.engine.iterators;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;

import java.util.List;
import java.util.Optional;

/**
 * A Bind Join specialized for Left join/Optionals
 * @author Thomas Minier
 */
public class OptionalBindJoinIterator extends BindJoinIterator {
    /**
     * Constructor
     * @param source     - Input for the left join
     * @param client     - HTTP client used to query the SaGe server
     * @param bgp        - Basic Graph pattern to left join with
     * @param bufferSize - Size of the bind join buffer (15 is the "default" admitted value)
     */
    public OptionalBindJoinIterator(QueryIterator source, SageRemoteClient client, BasicPattern bgp, int bufferSize) {
        super(source, client, bgp, bufferSize);
    }

    @Override
    protected void reviewResults(QueryResults results) {
        List<Binding> solutions = results.getBindings();
        if (solutions.isEmpty()) {
            // optional part: return buffer of input bindings
            bindingsBuffer.addAll(tempBuffer);
            hasNextPage = false;
            nextLink = Optional.empty();
        } else {
            // found results!
            bindingsBuffer.addAll(solutions);
            nextLink = results.getNext();
            hasNextPage = results.hasNext();
        }
    }
}
