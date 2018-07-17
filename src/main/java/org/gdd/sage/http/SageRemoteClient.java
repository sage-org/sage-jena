package org.gdd.sage.http;

import org.apache.jena.sparql.core.BasicPattern;
import org.gdd.sage.http.data.QueryResults;

import java.util.List;
import java.util.Optional;

public interface SageRemoteClient {
    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(BasicPattern bgp);

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(BasicPattern bgp, Optional<String> next);

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param patterns - List of BGPs to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(List<BasicPattern> patterns, Optional<String> next);

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param patterns - List of BGPs to evaluate
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(List<BasicPattern> patterns);

    /**
     * Free all resources used by the client
     */
    void close();

    /**
     * Get the URL of the remote sage server
     * @return The URL of the remote sage server
     */
    String getServerURL();
}
