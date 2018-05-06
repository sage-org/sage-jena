package org.gdd.sage.federated.factory;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.DatasetGraph;
import org.gdd.sage.federated.FederatedDatasetBuilder;
import org.gdd.sage.federated.selection.ServiceTransformer;
import org.gdd.sage.model.SageGraph;

import java.util.LinkedList;
import java.util.List;

/**
 * Factory used to build the execution envitonment for a federated SPARQL query.
 * For a query with SERVICE clauses, this factory generate the associated localized query and the
 * DatasetGraph that holds the graphs of the federation.
 * @author Thomas Minier
 */
public class ServiceFederatedQueryFactory implements FederatedQueryFactory {
    private String defaultUrl;
    private Query query;
    private Op localizedQuery;
    private DatasetGraph federation;
    private List<String> uris;

    public ServiceFederatedQueryFactory(String defaultUrl, Query query) {
        this.defaultUrl = defaultUrl;
        this.query = query;
        this.uris = new LinkedList<>();
    }

    @Override
    public void buildFederation() {
        // localize query and get all SERVICE uris
        Op queryTree = Algebra.compile(query);
        ServiceTransformer transformer = new ServiceTransformer();
        localizedQuery = Transformer.transform(transformer, queryTree);
        uris.addAll(transformer.getUris());

        // build the federated dataset
        Graph defaultGraph = new SageGraph(defaultUrl);
        FederatedDatasetBuilder builder = FederatedDatasetBuilder.create(defaultGraph);
        for (String uri: uris) {
            builder = builder.withSageServer(uri);
        }
        federation = builder.create();
    }

    @Override
    public Op getLocalizedQuery() {
        return localizedQuery;
    }

    @Override
    public DatasetGraph getFederationDataset() {
        return federation;
    }
}
