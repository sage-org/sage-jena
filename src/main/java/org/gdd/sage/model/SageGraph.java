package org.gdd.sage.model;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.gdd.sage.engine.iterators.SageBGPIterator;
import org.gdd.sage.http.SageClientBuilder;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Represents a remote RDF graph hosted at a Sage server
 * @author Thomas Minier
 */
public class SageGraph extends GraphBase {
    private SageRemoteClient httpClient;

    /**
     * Constructor
     * @param url - URL of the SaGe server
     */
    public SageGraph(String url) {
        super();
        this.httpClient = SageClientBuilder.createDefault(url);
    }

    public String getServerURL() {
        return httpClient.getServerURL();
    }

    public SageRemoteClient getClient() {
        return httpClient;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        BasicPattern bgp = new BasicPattern();
        bgp.add(triple);
        QueryIterator queryIterator = new SageBGPIterator(httpClient, bgp);
        return WrappedIterator.create(queryIterator).mapWith(binding -> Substitute.substitute(triple, binding));
    }

    @Override
    public void close() {
        super.close();
        this.httpClient.close();
    }

    /**
     * Evaluate a Basic Graph Pattern using the SaGe server
     * @param bgp - BGP to evaluate
     * @return An iterator over solution bindings for the BGP
     */
    public QueryIterator basicGraphPatternFind(BasicPattern bgp) {
        return new SageBGPIterator(httpClient, bgp);
    }

    /**
     * Get metadata about a Basic Graph Pattern
     * @param bgp - BGP to describe
     * @return Metadata about the BGP
     */
    public QueryResults analyze(BasicPattern bgp) {
        return httpClient.query(bgp);
    }
}
