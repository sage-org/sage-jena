package org.gdd.sage.federated;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphMapLink;
import org.gdd.sage.model.SageGraph;

public class FederatedDatasetBuilder {
    private DatasetGraph federation;
    
    private FederatedDatasetBuilder(Graph defaultGraph) {
        federation = new DatasetGraphMapLink(defaultGraph);
    }

    public static FederatedDatasetBuilder create(Graph defaultGraph) {
        return new FederatedDatasetBuilder(defaultGraph);
    }

    public DatasetGraph create() {
        return federation;
    }

    public FederatedDatasetBuilder withSageServer(String url) {
        federation.addGraph(NodeFactory.createURI(url), new SageGraph(url));
        return this;
    }

    public FederatedDatasetBuilder withGraph(Node uri, Graph graph) {
        federation.addGraph(uri, graph);
        return this;
    }

    public FederatedDatasetBuilder withGraph(String uri, Graph graph) {
        return withGraph(NodeFactory.createURI(uri), graph);
    }
}
