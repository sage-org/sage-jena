package org.gdd.sage.engine.iterators.boundjoin;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.SageRemoteClient;

import java.util.List;
import java.util.Map;

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
    public OptionalBoundJoinIterator(QueryIterator source, SageRemoteClient client, BasicPattern bgp, int bufferSize, ExecutionContext context) {
        super(source, client, bgp, bufferSize, context);
    }

    @Override
    protected QueryIterator createIterator(List<BasicPattern> bag, List<Binding> block, Map<Integer, Binding> rewritingMap, boolean isContainmentQuery) {
        return new OptionalRewriteIterator(client, bag, block, rewritingMap, isContainmentQuery);
    }
}
