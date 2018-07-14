package org.gdd.sage.engine.iterators;

import org.apache.jena.sparql.core.BasicPattern;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;

import java.util.List;
import java.util.Optional;

/**
 * An Iterator that evaluates a Union query query using a Sage server
 * @author Thomas Minier
 */
public class SageUnionIterator extends SageQueryIterator {

    private List<BasicPattern> patterns;

    public SageUnionIterator(SageRemoteClient client, List<BasicPattern> patterns) {
        super(client);
        this.patterns = patterns;
    }

    @Override
    protected QueryResults query(Optional<String> nextLink) {
        return client.query(patterns, nextLink);
    }
}
