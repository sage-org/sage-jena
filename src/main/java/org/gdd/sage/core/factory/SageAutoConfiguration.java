package org.gdd.sage.core.factory;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.gdd.sage.core.SageDatasetBuilder;
import org.gdd.sage.core.analyzer.FilterAnalyzer;
import org.gdd.sage.core.analyzer.FilterRegistry;
import org.gdd.sage.core.analyzer.ServiceAnalyzer;
import org.gdd.sage.http.ExecutionStats;
import org.gdd.sage.model.SageGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory used to build the execution environment for executing a SPARQL query with a Sage server
 * For a query with SERVICE clauses, this factory generate the associated localized query and the
 * DatasetGraph that holds the graphs of the federation.
 * @author Thomas Minier
 */
public class SageAutoConfiguration implements SageConfigurationFactory {
    private String defaultUrl;
    private Query query;
    private Dataset federation;
    private Set<String> uris;
    private ExecutionStats spy;
    private FilterRegistry filters;

    public SageAutoConfiguration(String defaultUrl, Query query) {
        this.defaultUrl = defaultUrl;
        this.query = query;
        this.uris = new HashSet<>();
        spy = new ExecutionStats();
    }

    public SageAutoConfiguration(String defaultUrl, Query query, ExecutionStats spy) {
        this.defaultUrl = defaultUrl;
        this.query = query;
        this.uris = new HashSet<>();
        this.spy = spy;
    }

    @Override
    public void buildDataset() {
        // localize query and get all SERVICE uris
        Op queryTree = Algebra.compile(query);
        ServiceAnalyzer transformer = new ServiceAnalyzer();
        Transformer.transform(transformer, queryTree);

        uris.addAll(transformer.getUris());

        // collect filters by variable
        FilterAnalyzer fAnalyzer = new FilterAnalyzer();
        Transformer.transform(fAnalyzer, queryTree);
        filters = fAnalyzer.getFilters();

        // build the federated dataset
        Graph defaultGraph = new SageGraph(defaultUrl, spy);
        SageDatasetBuilder builder = SageDatasetBuilder.create(defaultGraph);
        for (String uri: uris) {
            builder = builder.withSageServer(uri, spy);
        }
        federation = builder.create();
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public Dataset getDataset() {
        return federation;
    }

    public FilterRegistry getFilters() {
        return filters;
    }
}
