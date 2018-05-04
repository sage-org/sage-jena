package org.gdd.sage.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.gdd.sage.http.data.QueryResults;
import org.gdd.sage.http.data.SageQueryBuilder;
import org.gdd.sage.http.data.SageResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Allows evaluation of Basic Graph Patterns against a SaGe server
 * @author Thomas Minier
 */
public class SageDefaultClient implements SageRemoteClient {
    private String serverURL;
    private HttpClient httpClient;
    private ExecutorService threadPool;
    private ObjectMapper mapper;
    private Map<String, QueryResults> bgpCache;

    /**
     * Constructor
     * @param url - URL of the SaGe server
     * @param client - HTTP client used to perform HTTP requests
     */
    public SageDefaultClient(String url, HttpClient client) {
        serverURL = url;
        httpClient = client;
        threadPool = Executors.newCachedThreadPool();
        mapper = new ObjectMapper();
        bgpCache = new LRUCache<>(1000);
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param bgp - BGP to evaluate
     * @param next - (optional) Link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     * @throws IOException
     */
    public Future<QueryResults> query(BasicPattern bgp, String next) {
        String jsonQuery = SageQueryBuilder.builder()
                .withType("bgp")
                .withBasicGraphPattern(bgp)
                .withNextLink(next)
                .build();
        if (bgpCache.containsKey(jsonQuery)) {
            return CompletableFuture.completedFuture(bgpCache.get(jsonQuery));
        }
        return threadPool.submit(() -> {
            QueryResults qResults = null;
            try {
                qResults = decodeResponse(sendQuery(jsonQuery));
            } catch (IOException e) {
                // TODO handle errors
                return null;
            }
            bgpCache.put(jsonQuery, qResults);
            return qResults;
        });
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     * @throws IOException
     */
    public Future<QueryResults> query(BasicPattern bgp) {
        return query(bgp, null);
    }

    /**
     * Send an HTTP POST query to the SaGe Server
     * @param jsonQuery - JSOn query
     * @return The HTTP Response received from the server
     * @throws IOException
     */
    private HttpResponse sendQuery(String jsonQuery) throws IOException {
        HttpPost query = new HttpPost(this.serverURL);
        query.setHeader("accept", "application/json");
        query.setHeader("content-type", "application/json");
        query.setEntity(new StringEntity(jsonQuery));
        return httpClient.execute(query);
    }

    /**
     * Decode an HTTP response from a SaGe server
     * @param response - The HTTP response to decode
     * @return A decoded response
     * @throws IOException
     */
    private QueryResults decodeResponse(HttpResponse response) throws IOException {
        HttpEntity resEntity = response.getEntity();
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            throw new ClientProtocolException("Unexpected error when executing HTTP request: " + EntityUtils.toString(resEntity));
        }
        SageResponse sageResponse = mapper.readValue(EntityUtils.toString(resEntity), new TypeReference<SageResponse>(){});
        EntityUtils.consume(resEntity);

        // format bindings in Jena format
        List<Binding> results = sageResponse.bindings.parallelStream().map(binding -> {
            BindingHashMap b = new BindingHashMap();
            for (Map.Entry<String, String> entry: binding.entrySet()) {
                Var key = Var.alloc(entry.getKey().substring(1));
                Node value;
                if (entry.getValue().startsWith("\"")) {
                    value = NodeFactoryExtra.parseNode(entry.getValue());
                } else {
                    value = NodeFactory.createURI(entry.getValue());
                }
                b.add(key, value);
            }
            return b;
        }).collect(Collectors.toList());
        return new QueryResults(results, sageResponse.next, sageResponse.stats);
    }
}
