package org.gdd.sage.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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
import java.util.stream.Collectors;

/**
 * Allows evaluation of Basic Graph Patterns against a SaGe server
 * @author Thomas Minier
 */
public class SageDefaultClient implements SageRemoteClient {
    private String serverURL;
    private HttpClient httpClient;
    private ObjectMapper mapper;

    /**
     * Constructor
     * @param url - URL of the SaGe server
     * @param client - HTTP client used to perform HTTP requests
     */
    public SageDefaultClient(String url, HttpClient client) {
        serverURL = url;
        httpClient = client;
        mapper = new ObjectMapper();
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with a next link
     * @param bgp - BGP to evaluate
     * @param next - (optional) Link used to resume query evaluation
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     * @throws IOException
     */
    public QueryResults query(BasicPattern bgp, String next) throws IOException {
        HttpResponse response = sendQuery(bgp, next);
        SageResponse sageResponse = decodeResponse(response);

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
        return new QueryResults(results, sageResponse.next);
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, without a next link
     * @param bgp - BGP to evaluate
     * @return Query results. If the next link is null, then the BGP has been completely evaluated.
     * @throws IOException
     */
    public QueryResults query(BasicPattern bgp) throws IOException {
        return query(bgp, null);
    }

    /**
     * Send an HTTP POST query to the SaGe Server
     * @param bgp - BGP to evaluate
     * @param next - Next link (may be null)
     * @return The HTTP Response received from the server
     * @throws IOException
     */
    private HttpResponse sendQuery(BasicPattern bgp, String next) throws IOException {
        HttpPost query = new HttpPost(this.serverURL);
        query.setHeader("accept", "application/json");
        query.setHeader("content-type", "application/json");

        String q = SageQueryBuilder.builder()
                .withType("bgp")
                .withBasicGraphPattern(bgp)
                .withNextLink(next)
                .build();
        query.setEntity(new StringEntity(q));
        return httpClient.execute(query);
    }

    /**
     * Decode an HTTP response from a SaGe server
     * @param response - The HTTP response to decode
     * @return A decoded response
     * @throws IOException
     */
    private SageResponse decodeResponse(HttpResponse response) throws IOException {
        HttpEntity resEntity = response.getEntity();
        SageResponse sageResponse = mapper.readValue(EntityUtils.toString(resEntity), new TypeReference<SageResponse>(){});
        EntityUtils.consume(resEntity);
        return sageResponse;
    }
}
