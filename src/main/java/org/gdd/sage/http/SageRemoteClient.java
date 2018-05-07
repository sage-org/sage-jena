package org.gdd.sage.http;

import org.apache.jena.sparql.core.BasicPattern;
import org.gdd.sage.http.data.QueryResults;

import java.util.Optional;
import java.util.concurrent.Future;

public interface SageRemoteClient {
    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    Future<QueryResults> query(BasicPattern bgp);

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    Future<QueryResults> query(BasicPattern bgp, Optional<String> next);

    /**
     * Free all resources used by the client
     */
    void close();

    String getServerURL();
}
