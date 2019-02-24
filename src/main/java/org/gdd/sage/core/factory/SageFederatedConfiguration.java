package org.gdd.sage.core.factory;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.gdd.sage.core.SageDatasetBuilder;
import org.gdd.sage.engine.SageOpExecutorFactory;
import org.gdd.sage.federated.SourceSelection;
import org.gdd.sage.http.ExecutionStats;
import org.gdd.sage.model.SageGraph;

import java.util.List;

/**
 * Build the execution environment for executing a federated SPARQL query over a set of sources,
 * with a phase of source selection beforehand.
 * @author Thomas Minier
 */
public class SageFederatedConfiguration implements SageConfigurationFactory {
    private String defaultUrl;
    private List<String> urls;
    private Query query;
    private Dataset federation;
    private ExecutionStats spy;

    /**
     * Constructor
     * @param urls - Set of sources
     * @param query - SPARQL query
     */
    public SageFederatedConfiguration(List<String> urls, Query query) {
        defaultUrl = urls.get(0);
        this.urls = urls;
        this.query = query;
        this.spy = new ExecutionStats();
    }

    /**
     * Constructor
     * @param urls - Set of sources
     * @param query - SPARQL query
     * @param spy - Spy used to record execution statistics
     */
    public SageFederatedConfiguration(List<String> urls, Query query, ExecutionStats spy) {
        defaultUrl = urls.get(0);
        this.urls = urls;
        this.query = query;
        this.spy = spy;
    }

    @Override
    public void configure() {
        OpExecutorFactory opFactory = new SageOpExecutorFactory();
        QC.setFactory(ARQ.getContext(), opFactory);
    }

    @Override
    public void buildDataset() {
        // build the federated dataset
        SageGraph defaultGraph = new SageGraph(defaultUrl, spy);

        SageDatasetBuilder builder = SageDatasetBuilder.create(defaultGraph);
        for (String graphURI: urls) {
            builder = builder.withSageServer(graphURI, spy);
        }
        federation = builder.create();

        // configure the source selection
        SourceSelection sourceSelection = new SourceSelection();
        sourceSelection.registerSource(defaultUrl, defaultGraph.getClient());
        for (String graphURI: urls) {
            SageGraph namedGraph = (SageGraph) federation.getNamedModel(graphURI).getGraph();
            sourceSelection.registerSource(graphURI, namedGraph.getClient());
        }

        // perform source selection and rewrite query
        Op root = sourceSelection.localize(query);
        query = OpAsQuery.asQuery(root);
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public Dataset getDataset() {
        return federation;
    }
}
