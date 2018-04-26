package org.gdd.sage.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.util.NodeFactoryExtra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Allows evaluation of Basic Graph Patterns against a SaGe server
 * @author Thomas Minier
 */
public class SageRemoteClient {
    private String serverURL;
    private CloseableHttpClient httpClient;
    private ObjectMapper mapper;

    /**
     * Constructor
     * @param url - URL of the SaGe server
     */
    public SageRemoteClient(String url) {
        serverURL = url;
        httpClient = HttpClients.createDefault();
        mapper = new ObjectMapper();
    }

    /**
     * Evaluate a Basic Graph Pattern against a SaGe server, with an optional next link
     * @param bgp - BGP to evaluate
     * @param next - (optional) Link used to resume query evaluation
     * @return A 2-Tuple (query results, next link). If the next link is null, then the BGP has been completely evaluated.
     * @throws IOException
     */
    public QueryResults query(BasicPattern bgp, String next) throws IOException {
        List<Binding> results = new ArrayList<>();
        CloseableHttpResponse response = sendQuery(bgp, next);
        SageResponse sageResponse = decodeResponse(response);
        response.close();

        // format bindings in Jena format
        for (Map<String, String> binding: sageResponse.bindings) {
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
            results.add(b);
        }
        return new QueryResults(results, sageResponse.next);
    }

    /**
     * Send an HTTP POST query to the SaGe Server
     * @param bgp - BGP to evaluate
     * @param next - Next link (may be null)
     * @return The HTTP Response received from the server
     * @throws IOException
     */
    private CloseableHttpResponse sendQuery(BasicPattern bgp, String next) throws IOException {
        HttpPost query = new HttpPost(this.serverURL);
        query.setHeader("accept", "application/json");
        query.setHeader("content-type", "application/json");

        String q = SageQuery.toJSONString("bgp", bgp.getList(), next);
        query.setEntity(new StringEntity(q));
        return httpClient.execute(query);
    }

    /**
     * Decode an HTTP response from a SaGe server
     * @param response - The HTTP response to decode
     * @return A decoded response
     * @throws IOException
     */
    private SageResponse decodeResponse(CloseableHttpResponse response) throws IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        return mapper.readValue(rd, new TypeReference<SageResponse>(){});
    }

    public static void main(String[] args) {
        BasicPattern bgp = new BasicPattern();

        Triple triple1 = new Triple(
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272"),
                NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer"),
                Var.alloc("producer"));
        Triple triple2 = new Triple(
                Var.alloc("producer"),
                NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
                new Node_Variable("label"));
        bgp.add(triple1);
        bgp.add(triple2);

        SageRemoteClient client = new SageRemoteClient("http://localhost:8000/sparql/bsbm1k");

        try {
            QueryResults results = client.query(bgp, null);
            System.out.println(results.bindings);
            System.out.println("nb results: " + results.bindings.size());
            System.out.println("Next link: " + results.next);
            System.out.println("has next? " + results.hasNext());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
