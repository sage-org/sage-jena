package org.gdd.sage.model;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.gdd.sage.engine.iterators.SageBGPIterator;
import org.gdd.sage.engine.iterators.SageUnionIterator;
import org.gdd.sage.http.ExecutionStats;
import org.gdd.sage.http.SageDefaultClient;
import org.gdd.sage.http.SageRemoteClient;

import java.util.List;
import java.util.Map;

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
        this.httpClient = new SageDefaultClient(url);
    }

    /**
     * Constructor
     * @param url - URL of the SaGe server
     */
    public SageGraph(String url, ExecutionStats spy) {
        super();
        this.httpClient = new SageDefaultClient(url, spy);
    }

    /**
     * Get the URL of the remote sage server
     * @return The URL of the remote sage server
     */
    public String getServerURL() {
        return httpClient.getServerURL();
    }

    /**
     * Get the HTTP client used to access the remote Sage server
     * @return The HTTP client used to access the remote Sage server
     */
    public SageRemoteClient getClient() {
        return httpClient;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        // Jena may inject strange "ANY" that are not labelled as variable when evaluating property paths
        // so we need to sanitize the triple pattern before evaluation
        Node s = triple.getSubject();
        Node p = triple.getPredicate();
        Node o = triple.getObject();
        if (s.toString().equals("ANY")) {
            s = Var.alloc("ANY_S");
        }
        if (p.toString().equals("ANY")) {
            p = Var.alloc("ANY_P");
        }
        if (o.toString().equals("ANY")) {
            o = Var.alloc("ANY_O");
        }
        // evaluate formatted triple pattern
        Triple t = new Triple(s, p, o);
        BasicPattern bgp = new BasicPattern();
        bgp.add(t);
        QueryIterator queryIterator = new SageBGPIterator(httpClient, bgp);
        return WrappedIterator.create(queryIterator)
                .mapWith(binding -> Substitute.substitute(t, binding));
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
     * Evaluate a Basic Graph Pattern using the SaGe server
     * @param bgp - BGP to evaluate
     * @return An iterator over solution bindings for the BGP
     */
    public QueryIterator basicGraphPatternFind(BasicPattern bgp, String filter) {
        if (filter.isEmpty()) {
            return new SageBGPIterator(httpClient, bgp);
        }
        return new SageBGPIterator(httpClient, bgp, filter);
    }

    /**
     * Evaluate an Union of BGPs using the SaGe server
     * @param patterns - Union to evaluate
     * @return An iterator over solution bindings for the Union
     */
    public QueryIterator unionFind(List<BasicPattern> patterns) {
        return new SageUnionIterator(httpClient, patterns);
    }
}
