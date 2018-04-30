package org.gdd.sage.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.data.QueryResults;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class SageDefaultClientTest {
    private SageRemoteClient sageClient;

    @Before
    public void setUp() throws Exception {
        HttpClient mockedClient = mock(HttpClient.class);
        HttpResponse mockedResponse = mock(HttpResponse.class);
        HttpEntity mockedEntity = mock(HttpEntity.class);

        InputStream jsonStream = new FileInputStream("src/test/resources/json_response.json");
        when(mockedEntity.getContent()).thenReturn(jsonStream);
        when(mockedResponse.getEntity()).thenReturn(mockedEntity);
        when(mockedClient.execute(any())).thenReturn(mockedResponse);

        sageClient = SageClientBuilder.create()
                .withURL("http://foo.bar/sparql/bsbsm")
                .withHTTPClient(mockedClient)
                .build();
    }

    @Test
    public void query() {
        List<String> vars = Arrays.asList("comment", "f", "label", "producer",
                "productFeature", "propertyNumeric1", "propertyNumeric2", "propertyTextual1",
                "propertyTextual2", "propertyTextual3");
        try {
            BasicPattern bgp = new BasicPattern();
            QueryResults results = sageClient.query(bgp);
            assertEquals("QueryResults should have 9 solution bindings", 9, results.bindings.size());
            assertFalse("QueryResults should not have a next page", results.hasNext());
            for (Binding binding: results.bindings) {
                for(String var: vars) {
                    assertTrue(binding.contains(Var.alloc(var)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}