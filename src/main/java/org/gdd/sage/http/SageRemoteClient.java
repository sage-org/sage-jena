package org.gdd.sage.http;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.expr.Expr;
import org.gdd.sage.http.data.QueryResults;

import java.util.List;
import java.util.Optional;

/**
 * Generic interface for an HTTP client that sends SPARQL queries to a Sage server.
 * @author Thomas Minier
 */
public interface SageRemoteClient {
    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp);

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, Optional<String> next);

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expression
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters);

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expressions
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters, Optional<String> next);

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, List<BasicPattern> patterns);

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, List<BasicPattern> patterns, Optional<String> next);

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
