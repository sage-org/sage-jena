package org.gdd.sage.model;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.gdd.sage.engine.SageBGPIterator;
import org.gdd.sage.http.SageRemoteClient;

public class SageGraph extends GraphBase {
    private String url;
    private SageRemoteClient httpClient;

    public SageGraph(String url) {
        super();
        this.url = url;
        this.httpClient = new SageRemoteClient(this.url);
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        return null;
    }

    public QueryIterator evaluateBGP(BasicPattern bgp) {
        return new SageBGPIterator(httpClient, bgp);
    }
}
