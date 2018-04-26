package org.gdd.sage.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.jena.graph.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.gdd.sage.http.SageQuery;
import org.gdd.sage.http.SageResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
     * Send an HTTP POST query to the SaGe Server
     * @param bgp - BGP to evaluate
     * @param next - Next link (may be null)
     * @return The HTTP Response received from the server
     * @throws IOException
     */
    private CloseableHttpResponse doQuery(BasicPattern bgp, String next) throws IOException {
        HttpPost query = new HttpPost(this.serverURL);
        query.setHeader("accept", "application/json");
        query.setHeader("content-type", "application/json");

        SageQuery q = new SageQuery(bgp.getList(), next);
        query.setEntity(new StringEntity(q.toJSONString()));
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
        // create HTTP client
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost query = new HttpPost(
                "http://localhost:8000/sparql/bsbm1k");
        query.setHeader("accept", "application/json");
        query.setHeader("content-type", "application/json");
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
        SageQuery q = new SageQuery(bgp.getList(), null);
        try {
            query.setEntity(new StringEntity(q.toJSONString()));
            // do POST query and read results
            CloseableHttpResponse response = httpclient.execute(query);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            ObjectMapper mapper = new ObjectMapper();

            SageResponse queryResponse = mapper.readValue(rd, new TypeReference<SageResponse>(){});
            response.close();

            // format bindings in Jena format
            List<Binding> results = new ArrayList<>();
            for (Map<String, String> binding: queryResponse.bindings) {
                BindingHashMap b = new BindingHashMap();
                for (Map.Entry<String, String> entry: binding.entrySet()) {
                    Var key = Var.alloc(entry.getKey().substring(1));
                    Node value;
                    if (entry.getValue().startsWith("\"")) {
                        value = NodeFactory.createLiteral(entry.getValue());
                    } else {
                        value = NodeFactory.createURI(entry.getValue());
                    }
                    b.add(key, value);
                }
                results.add(b);
            }
            System.out.println(results);
            System.out.println("Cardinality: " + queryResponse.pageSize);
            System.out.println("nb results: " + results.size());
            System.out.println("Next link: " + queryResponse.next);

        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
