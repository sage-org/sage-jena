package org.gdd.sage.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.vocabulary.XSD;
import org.gdd.sage.http.data.QueryResults;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
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

    @After
    public void tearDown() throws Exception {
        sageClient.close();
    }

    @Test
    public void query() {
        List<Var> vars = Stream.of("comment", "f", "p", "label", "producer",
                "productFeature", "propertyNumeric1", "propertyNumeric2", "propertyTextual1",
                "propertyTextual2", "propertyTextual3").map(Var::alloc).collect(Collectors.toList());
        try {
            BasicPattern bgp = new BasicPattern();
            QueryResults results = sageClient.query(bgp).get();
            assertFalse("A valid query should not have errors", results.hasError());
            assertEquals("Query results should contains exactly 9 solution bindings", 9, results.getBindings().size());
            assertFalse("Query results should not have a next page", results.hasNext());
            for (Binding binding: results.getBindings()) {
                assertEquals("Each set of bindings should bind to 11 variables", vars.size(), binding.size());
                for(Var var: vars) {
                    assertTrue(binding.contains(var));
                    Node currentNode = binding.get(var);
                    // check for binding's types and XSD datatypes (for literals)
                    if (var.getVarName().equals("f") || var.getVarName().equals("p")) {
                        assertTrue("?f and ?p should binds to URI", currentNode.isURI());
                    } else {
                        assertTrue("Variables others than ?f and ?p should bind to Literals", currentNode.isLiteral());
                        if (var.getVarName().startsWith("propertyNumeric")) {
                            assertEquals(
                                    "?propertyNumeric1 and ?propertyNumeric2 should bind to integer literals",
                                    currentNode.getLiteralDatatypeURI(), XSD.integer.getURI());
                        } else {
                            assertEquals(
                                    "?propertyTextual1, ?propertyTextual2 and ?propertyTextual3 should bind to string literals",
                                    currentNode.getLiteralDatatypeURI(), XSD.xstring.getURI());
                        }
                    }
                }
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