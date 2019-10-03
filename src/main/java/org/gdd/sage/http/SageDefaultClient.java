package org.gdd.sage.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import org.apache.commons.io.IOUtils;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.gdd.sage.engine.update.base.UpdateQuery;
import org.gdd.sage.http.cache.QueryCache;
import org.gdd.sage.http.cache.SimpleCache;
import org.gdd.sage.http.data.SageQueryBuilder;
import org.gdd.sage.http.data.SageResponse;
import org.gdd.sage.http.results.QueryResults;
import org.gdd.sage.http.results.UpdateResults;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Allows evaluation of SPARQL queries against a SaGe server.
 * For now, only BGP and UNION queries are supported.
 * @author Thomas Minier
 */
public class SageDefaultClient implements SageRemoteClient {
    private String serverURL;
    private ExecutorService threadPool;
    private ObjectMapper mapper;
    private HttpRequestFactory requestFactory;
    private ExecutionStats spy;
    private QueryCache cache = new SimpleCache(100);
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static final String HTTP_JSON_CONTENT_TYPE = "application/json";

    private class JSONPayload {
        private String query;
        private String defaultGraph;
        private String next = null;

        public JSONPayload(String defaultGraph, String query) {
            this.query = query;
            this.defaultGraph = defaultGraph;
        }

        public JSONPayload(String defaultGraph, String query, String next) {
            this.query = query;
            this.defaultGraph = defaultGraph;
            this.next = next;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getDefaultGraph() {
            return defaultGraph;
        }

        public void setDefaultGraph(String defaultGraph) {
            this.defaultGraph = defaultGraph;
        }

        public String getNext() {
            return next;
        }

        public void setNext(String next) {
            this.next = next;
        }
    }

    /**
     * Constructor
     * @param serverURL - URL of the SaGe server SPARQL service, e.g.,
     */
    public SageDefaultClient(String serverURL) {
        this.serverURL = serverURL;
        threadPool = Executors.newCachedThreadPool();
        mapper = new ObjectMapper();
        requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> {
            request.getHeaders().setAccept(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setContentType(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setUserAgent("Sage-Jena client/Java 1.8");
            request.setParser(new JsonObjectParser(JSON_FACTORY));
            request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()));
        });
        spy = new ExecutionStats();
    }

    /**
     * Constructor
     * @param serverURL - URL of the SaGe server
     */
    public SageDefaultClient(String serverURL, ExecutionStats spy) {
        this.serverURL = serverURL;
        threadPool = Executors.newCachedThreadPool();
        mapper = new ObjectMapper();
        requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> {
            request.getHeaders().setAccept(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setContentType(HTTP_JSON_CONTENT_TYPE);
            request.getHeaders().setUserAgent("Sage-Jena client/Java 1.8");
            request.setParser(new JsonObjectParser(JSON_FACTORY));
            request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()));
            request.setConnectTimeout(0);
            request.setReadTimeout(0);
        });
        this.spy = spy;
    }

    /**
     * Get the URL of the remote sage server
     * @return The URL of the remote sage server
     */
    public String getServerURL() {
        return serverURL;
    }

    private String buildJSONPayload(String graphURI, String query, Optional<String> next) {
        JSONPayload payload;
        if (next.isPresent()) {
            payload = new JSONPayload(graphURI, query, next.get());
        } else {
            payload = new JSONPayload(graphURI, query);
        }
        try {
            return mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return "";
        }
    }

    /**
     * Send a SPARQL query to the SaGe server
     * @param query - SPARQL query to send
     * @param graphURI - URI of the default graph
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    private QueryResults sendQuery(String graphURI, String query, Optional<String> next, boolean isRead) {
        // check in cache first
        if (isRead && cache.has(graphURI, query, next)) {
            return cache.get(graphURI, query, next);
        }
        // build POST query
        GenericUrl url = new GenericUrl(serverURL);
        String payload = buildJSONPayload(graphURI, query, next);
        HttpContent postContent = new ByteArrayContent(HTTP_JSON_CONTENT_TYPE, payload.getBytes());
        double startTime = System.nanoTime();
        try {
            HttpRequest request = requestFactory.buildPostRequest(url, postContent);
            HttpResponse response = request.executeAsync(threadPool).get();
            double endTime = System.nanoTime();
            if (isRead) {
                spy.reportHTTPQueryRead((endTime - startTime) / 1e9);
            } else {
                spy.reportHTTPQueryWrite((endTime - startTime) / 1e9);
            }
            return decodeResponse(response, isRead);
        } catch (InterruptedException | ExecutionException | IOException e) {
            double endTime = System.nanoTime();
            if (isRead) {
                spy.reportHTTPQueryRead((endTime - startTime) / 1e9);
            } else {
                spy.reportHTTPQueryWrite((endTime - startTime) / 1e9);
            }
            return QueryResults.withError(e.getMessage());
        }
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp) {
        return query(graphURI, bgp, Optional.empty());
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, Optional<String> next) {
        String query = SageQueryBuilder.buildBGPQuery(bgp);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate a Basic Graph Pattern with a GROUP BY against a SaGe server, without a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param variables - GROUP BY variables
     * @param aggregations - SPARQL aggregations (may be empty)
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults queryGroupBy(String graphURI, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions) {
        return queryGroupBy(graphURI, bgp, variables, aggregations, extensions, Optional.empty());
    }

    /**
     * Evaluate a Basic Graph Pattern with a GROUP BY against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param variables - GROUP BY variables
     * @param aggregations - SPARQL aggregations (may be empty)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults queryGroupBy(String graphURI, BasicPattern bgp, List<Var> variables, List<ExprAggregator> aggregations, VarExprList extensions,  Optional<String> next) {
        String query = SageQueryBuilder.buildBGPGroupByQuery(bgp, variables, aggregations, extensions);
        return sendQuery(graphURI, query, next,true);
    }

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expression
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters) {
        return query(graphURI, bgp, filters, Optional.empty());
    }

    /**
     * Evaluate a Basic Graph Pattern with filter against a SaGe server
     * @param graphURI - Default Graph URI
     * @param bgp - BGP to evaluate
     * @param filters - Filter expressions
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(String graphURI, BasicPattern bgp, List<Expr> filters, Optional<String> next) {
        String query = SageQueryBuilder.buildBGPQuery(bgp, filters);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, List<BasicPattern> patterns) {
        return query(graphURI, patterns, Optional.empty());
    }

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param graphURI - Default Graph URI
     * @param patterns - List of BGPs to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, List<BasicPattern> patterns, Optional<String> next) {
        String query = SageQueryBuilder.buildUnionQuery(patterns);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, Map<String, BasicPattern> graphs) {
        return query(graphURI, graphs, Optional.empty());
    }

    /**
     * Evaluate a set Graph clauses, each one wrapping a Basic Graph Patterns, against a SaGe server, with a next link.
     * @param graphURI - Default Graph URI
     * @param graphs - Graphs clauses to evaluates, i..e, tuples (graph uri, basic graph pattern)
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(String graphURI, Map<String, BasicPattern> graphs, Optional<String> next) {
        String query = SageQueryBuilder.buildGraphQuery(graphs);
        return sendQuery(graphURI, query, next, true);
    }

    /**
     * Evaluate a SPARQL UPDATE query using a {@link UpdateQuery} object
     * @param graphURI - Default Graph URI
     * @param query - Query to execute
     * @return Query results, containing the RDF quads that were processed by the server
     */
    public UpdateResults update(String graphURI, String query) {
        QueryResults results = sendQuery(graphURI, query, Optional.empty(), false);
        // convert QueryResults to UpdateResults
        if (results.hasError()) {
            return UpdateResults.withError(results.getError());
        }
        return new UpdateResults(graphURI, results.getBindings(), results.getStats());
    }

    /**
     * Free all resources used by the client
     */
    public void close() {
        threadPool.shutdown();
    }

    /**
     * Decode an HTTP response from a SaGe server
     * @param response - The HTTP response to decode
     * @return A decoded response
     * @throws IOException
     */
    private QueryResults decodeResponse(HttpResponse response, boolean isRead) throws IOException {
        String responseContent = IOUtils.toString(response.getContent(), Charset.forName("UTF-8"));

        int statusCode = response.getStatusCode();
        if (statusCode != 200) {
            throw new IOException("Unexpected error when executing HTTP request: " + responseContent);
        }
        SageResponse sageResponse = mapper.readValue(responseContent, new TypeReference<SageResponse>(){});
        if (isRead) {
            spy.reportOverheadRead(sageResponse.stats.getResumeTime(), sageResponse.stats.getSuspendTime());
        } else {
            spy.reportOverheadWrite(sageResponse.stats.getResumeTime(), sageResponse.stats.getSuspendTime());
        }
        return new QueryResults(sageResponse.bindings, sageResponse.next, sageResponse.stats);
    }
}
