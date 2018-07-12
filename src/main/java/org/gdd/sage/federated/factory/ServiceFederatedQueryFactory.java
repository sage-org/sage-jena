package org.gdd.sage.federated.factory;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.gdd.sage.federated.FederatedDatasetBuilder;
import org.gdd.sage.federated.selection.ServiceTransformer;
import org.gdd.sage.model.SageGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory used to build the execution environment for a federated SPARQL query.
 * For a query with SERVICE clauses, this factory generate the associated localized query and the
 * DatasetGraph that holds the graphs of the federation.
 * @author Thomas Minier
 */
public class ServiceFederatedQueryFactory implements FederatedQueryFactory {
    private String defaultUrl;
    private Query query;
    private Dataset federation;
    private Set<String> uris;

    public ServiceFederatedQueryFactory(String defaultUrl, Query query) {
        this.defaultUrl = defaultUrl;
        this.query = query;
        this.uris = new HashSet<>();
    }

    @Override
    public void buildFederation() {
        // localize query and get all SERVICE uris
        Op queryTree = Algebra.compile(query);
        ServiceTransformer transformer = new ServiceTransformer();
        Transformer.transform(transformer, queryTree);
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
    public Query getLocalizedQuery() {
        return query;
    }

    @Override
    public Dataset getFederationDataset() {
        return federation;
    }
}
