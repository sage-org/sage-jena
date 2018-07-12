package org.gdd.sage.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.commons.io.IOUtils;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.gdd.sage.http.data.QueryResults;
import org.gdd.sage.http.data.SageQueryBuilder;
import org.gdd.sage.http.data.SageResponse;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Allows evaluation of Basic Graph Patterns against a SaGe server
 * @author Thomas Minier
 */
public class SageDefaultClient implements SageRemoteClient {
    private GenericUrl serverURL;
    private ExecutorService threadPool;
    private ObjectMapper mapper;
    private HttpRequestFactory requestFactory;
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    /**
     * Constructor
     * @param url - URL of the SaGe server
     */
    public SageDefaultClient(String url) {
        serverURL = new GenericUrl(url);
        threadPool = Executors.newCachedThreadPool();
        mapper = new ObjectMapper();
        requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> {
            request.getHeaders().setAccept("application/json");
            request.getHeaders().setContentType("application/json");
            request.getHeaders().setUserAgent("Sage-Jena client/Java 1.8");
            request.setParser(new JsonObjectParser(JSON_FACTORY));
        });
    }

    public String getServerURL() {
        return serverURL.toString();
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(BasicPattern bgp, Optional<String> next) {
        SageQueryBuilder queryBuilder = SageQueryBuilder.builder()
                .withType("bgp")
                .withBasicGraphPattern(bgp);
        if (next.isPresent()) {
            queryBuilder = queryBuilder.withNextLink(next.get());
        }

        String jsonQuery = queryBuilder.build();
        try {
            HttpResponse response = sendQuery(jsonQuery).get();
            return decodeResponse(response);
        } catch (InterruptedException | ExecutionException | IOException e) {
            return QueryResults.withError(e.getMessage());
        }
    }

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param patterns - List of BGPs to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(List<BasicPattern> patterns, Optional<String> next) {
        SageQueryBuilder queryBuilder = SageQueryBuilder.builder()
                .withType("union")
                .withUnion(patterns);
        if (next.isPresent()) {
            queryBuilder = queryBuilder.withNextLink(next.get());
        }

        String jsonQuery = queryBuilder.build();
        try {
            HttpResponse response = sendQuery(jsonQuery).get();
            return decodeResponse(response);
        } catch (InterruptedException | ExecutionException | IOException e) {
            return QueryResults.withError(e.getMessage());
        }
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public QueryResults query(BasicPattern bgp) {
        return query(bgp, Optional.empty());
    }

    /**
     * Evaluate an Union of Basic Graph Patterns against a SaGe server, with a next link
     * @param patterns - List of BGPs to evaluate
     * @return Query results. If the next link is null, then the Union has been completely evaluated.
     */
    public QueryResults query(List<BasicPattern> patterns) {
        return query(patterns, Optional.empty());
    }

    /**
     * Free all resources used by the client
     */
    public void close() {
        threadPool.shutdown();
    }

    /**
     * Send an HTTP POST query to the SaGe Server
     * @param jsonQuery - JSOn query
     * @return The HTTP Response received from the server
     * @throws IOException
     */
    private Future<HttpResponse> sendQuery(String jsonQuery) throws IOException {
        HttpRequest request = requestFactory.buildPostRequest(serverURL, new ByteArrayContent("application/json", jsonQuery.getBytes()));
        return request.executeAsync(threadPool);
    }

    /**
     * Decode an HTTP response from a SaGe server
     * @param response - The HTTP response to decode
     * @return A decoded response
     * @throws IOException
     */
    private QueryResults decodeResponse(HttpResponse response) throws IOException {
        String responseContent = IOUtils.toString(response.getContent(), Charset.forName("UTF-8"));
        int statusCode = response.getStatusCode();
        if (statusCode != 200) {
            throw new IOException("Unexpected error when executing HTTP request: " + responseContent);
        }
        SageResponse sageResponse = mapper.readValue(responseContent, new TypeReference<SageResponse>(){});

        // format bindings in Jena format
        List<Binding> results = sageResponse.bindings.parallelStream().map(binding -> {
            BindingHashMap b = new BindingHashMap();
            for (Map.Entry<String, String> entry: binding.entrySet()) {
                try {
                    Var key = Var.alloc(entry.getKey().substring(1));
                    Node value;
                    if (entry.getValue().startsWith("http")) {
                        value = NodeFactory.createURI(entry.getValue());
                    } else {
                        String literal = entry.getValue().trim();
                        if (literal.contains("\"^^http")) {
                            int index = literal.lastIndexOf("\"^^http:");
                            value = NodeFactory.createLiteral(literal.substring(0, index), new BaseDatatype(literal.substring(index + 3)));
                        } else if (literal.contains("\"@")) {
                            int index = literal.lastIndexOf("\"@");
                            value = NodeFactory.createLiteral(literal.substring(0, index), literal.substring(index + 2));
                        } else {
                            value = NodeFactory.createLiteral(literal);
                        }
                    }
                    b.add(key, value);
                } catch(RiotParseException e) {
                    // TODO: for now we skip parsing errors, maybe need to do something cleaner
                }
            }
            return b;
        }).collect(Collectors.toList());
        return new QueryResults(results, sageResponse.next, sageResponse.stats);
    }
}
