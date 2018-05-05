package org.gdd.sage.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.data.QueryResults;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        List<Var> vars = Stream.of("comment", "f", "label", "producer",
                "productFeature", "propertyNumeric1", "propertyNumeric2", "propertyTextual1",
                "propertyTextual2", "propertyTextual3").map(Var::alloc).collect(Collectors.toList());
        try {
            BasicPattern bgp = new BasicPattern();
            QueryResults results = sageClient.query(bgp).get();
            assertFalse("A valid query should not have errors", results.hasError());
            assertEquals("Query results should contains exactly 9 solution bindings", 9, results.getBindings().size());
            assertFalse("Query results should not have a next page", results.hasNext());
            for (Binding binding: results.getBindings()) {
                for(Var var: vars) {
                    assertTrue(binding.contains(var));
                }
                //assertTrue(binding.get(Var.alloc("")));
            }
        } catch (InterruptedException | ExecutionException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void queryFailed() {
        clearInvocations(okStatus);
        when(okStatus.getStatusCode()).thenReturn(404);
        BasicPattern bgp = new BasicPattern();
        try {
            QueryResults queryResults = sageClient.query(bgp).get();
            assertTrue("A 404 response should cause an internal error", queryResults.hasError());
        } catch (InterruptedException | ExecutionException e) {
            fail(e.getMessage());
        }
    }
}