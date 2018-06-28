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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
     * Constructor
     * @param url - URL of the SaGe server
     * @param client - HTTP client used to perform HTTP requests
     */
    public SageDefaultClient(String url, HttpClient client, ExecutorService threadPool) {
        serverURL = url;
        httpClient = client;
        this.threadPool = threadPool;
        mapper = new ObjectMapper();
        bgpCache = new LRUCache<>(1000);
    }

    public String getServerURL() {
        return serverURL;
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param bgp - BGP to evaluate
     * @param next - Optional link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public Future<QueryResults> query(BasicPattern bgp, Optional<String> next) {
        System.out.println(bgp);
        System.out.println(next);

        SageQueryBuilder queryBuilder = SageQueryBuilder.builder()
                .withType("bgp")
                .withBasicGraphPattern(bgp);
        if (next.isPresent()) {
            queryBuilder = queryBuilder.withNextLink(next.get());
        }

        String jsonQuery = queryBuilder.build();
        System.out.println(jsonQuery);
        System.out.println("---------------------------");
        if (bgpCache.containsKey(jsonQuery)) {
            return CompletableFuture.completedFuture(bgpCache.get(jsonQuery));
        }
        return threadPool.submit(() -> {
            try {
                QueryResults qResults = decodeResponse(sendQuery(jsonQuery));
                bgpCache.put(jsonQuery, qResults);
                return qResults;
            } catch (IOException e) {
                return QueryResults.withError(e.getMessage());
            }
        });
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     */
    public Future<QueryResults> query(BasicPattern bgp) {
        return query(bgp, Optional.empty());
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
    private HttpResponse sendQuery(String jsonQuery) throws IOException {
        HttpPost query = new HttpPost(this.serverURL);
        query.setHeader("accept", "application/json; charset=utf-8");
        query.setHeader("content-type", "application/json; charset=utf-8");
        query.setEntity(new StringEntity(jsonQuery, Charset.forName("UTF-8")));
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
                if (entry.getValue().startsWith("http")) {
                    value = NodeFactory.createURI(entry.getValue());
                } else {
                    value = NodeFactoryExtra.parseNode(entry.getValue());
                }
                b.add(key, value);
            }
            return b;
        }).collect(Collectors.toList());
        return new QueryResults(results, sageResponse.next, sageResponse.stats);
    }
}
