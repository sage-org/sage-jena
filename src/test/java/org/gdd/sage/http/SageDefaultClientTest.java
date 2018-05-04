package org.gdd.sage.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.data.QueryResults;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class SageDefaultClientTest {
    private SageRemoteClient sageClient;
    private StatusLine okStatus;

    @Before
    public void setUp() throws Exception {
        HttpClient mockedClient = mock(HttpClient.class);
        HttpResponse mockedResponse = mock(HttpResponse.class);
        HttpEntity mockedEntity = mock(HttpEntity.class);
        okStatus = mock(StatusLine.class);

        when(okStatus.getStatusCode()).thenReturn(200);
        InputStream jsonStream = new FileInputStream("src/test/resources/json_response.json");
        when(mockedEntity.getContent()).thenReturn(jsonStream);
        when(mockedResponse.getStatusLine()).thenReturn(okStatus);
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
            QueryResults results = sageClient.query(bgp).get();
            assertEquals("QueryResults should have 9 solution bindings", 9, results.bindings.size());
            assertFalse("QueryResults should not have a next page", results.hasNext());
            for (Binding binding: results.bindings) {
                for(String var: vars) {
                    assertTrue(binding.contains(Var.alloc(var)));
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            fail(e.getMessage());
        }
    }

    @Ignore
    @Test(expected = ClientProtocolException.class)
    public void queryFailed() throws IOException {
        clearInvocations(okStatus);
        when(okStatus.getStatusCode()).thenReturn(404);
        BasicPattern bgp = new BasicPattern();
        sageClient.query(bgp);
        fail("A 404 response should cause an internal error");
    }
}