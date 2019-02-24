package org.gdd.sage.engine.iterators;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.expr.Expr;
import org.gdd.sage.engine.iterators.base.SageQueryIterator;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;

import java.util.List;
import java.util.Optional;

/**
 * Like a {@link SageBGPIterator}, but also apply a set of filters on the BGP
 * @author Thomas Minier
 */
public class SageFilterBGPIterator extends SageQueryIterator {
    private BasicPattern bgp;
    private List<Expr> filters;

    public SageFilterBGPIterator(String graphURI, SageRemoteClient client, BasicPattern bgp, List<Expr> filters) {
        super(graphURI, client);
        this.bgp = bgp;
        this.filters = filters;
    }

    @Override
    protected QueryResults query(Optional<String> nextLink) {
        return getClient().query(getGraphURI(), bgp, filters, nextLink);
    }
}
