package org.gdd.sage.http;

import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.expr.Expr;
import org.gdd.sage.engine.update.base.UpdateQuery;
import org.gdd.sage.http.results.QueryResults;
import org.gdd.sage.http.results.UpdateResults;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic interface for an HTTP client that sends SPARQL queries to a Sage server.
 * @author Thomas Minier
 */
public interface SageRemoteClient {
    /**
     * Get the URL of the remote sage server
     * @return The URL of the remote sage server
     */
    String getServerURL();

    /**
     * Free all resources used by the client
     */
    void close();

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
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, Map<String, BasicPattern> graphs);

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server, with a next link.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    QueryResults query(String graphURI, Map<String, BasicPattern> graphs, Optional<String> next);

    /**
     * Evaluate a SPARQL UPDATE query using a {@link UpdateQuery} object
     * @param graphURI - Default Graph URI
     * @param query - Query to execute
     * @return Query results, containing the RDF quads that were processed by the server
     */
    UpdateResults update(String graphURI, String query);
}
