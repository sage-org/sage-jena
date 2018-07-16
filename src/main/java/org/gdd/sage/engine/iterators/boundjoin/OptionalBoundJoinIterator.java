package org.gdd.sage.engine.iterators.boundjoin;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.engine.iterators.boundjoin.BoundJoinIterator;
import org.gdd.sage.http.SageRemoteClient;

import java.util.List;
import java.util.Optional;

/**
 * A special Bound Join for Left joins/Optional clauses
 * @author Thomas Minier
 */
public class OptionalBoundJoinIterator extends BoundJoinIterator {
    /**
     * Constructor
     * @param source     - Input for the left join
     * @param client     - HTTP client used to query the SaGe server
     * @param bgp        - Basic Graph pattern to left join with
     * @param bufferSize - Size of the bound join buffer (15 is the "default" admitted value)
     */
    public OptionalBoundJoinIterator(QueryIterator source, SageRemoteClient client, BasicPattern bgp, int bufferSize) {
        super(source, client, bgp, bufferSize);
    }

    @Override
    protected List<Binding> rewriteSolutions(List<Binding> input) {
        if (input.isEmpty()) {
            // optional part: avoid rewriting and simply return the bucket of bindings
            hasNextPage = false;
            nextLink = Optional.empty();
            return bindingBucket;
        }
        return super.rewriteSolutions(input);
    }
}
