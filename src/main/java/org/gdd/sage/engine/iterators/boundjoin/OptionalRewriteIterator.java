package org.gdd.sage.engine.iterators.boundjoin;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.SageRemoteClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OptionalRewriteIterator extends RewriteIterator {
    public OptionalRewriteIterator(SageRemoteClient client, List<BasicPattern> bag, List<Binding> block, Map<Integer, Binding> rewritingMap, boolean isContainmentQuery) {
        super(client, bag, block, rewritingMap, isContainmentQuery);
    }

    @Override
    protected List<Binding> rewriteSolutions(List<Binding> input) {
        if (input.isEmpty()) {
            // optional part: avoid rewriting and simply return the bucket of bindings
            hasNextPage = false;
            nextLink = Optional.empty();
            return getBlock();
        }
        return super.rewriteSolutions(input);
    }
}
