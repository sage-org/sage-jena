package org.gdd.sage.core;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetImpl;
import org.gdd.sage.http.ExecutionStats;
import org.gdd.sage.model.SageGraph;

/**
 * Builder used to create a new Dataset of Sage servers
 * @author Thomas Minier
 */
public class SageDatasetBuilder {
    private Dataset federation;

    private SageDatasetBuilder(Graph defaultGraph) {
        federation = new DatasetImpl(ModelFactory.createModelForGraph(defaultGraph));
    }

    private SageDatasetBuilder(Model defaultModel) {
        federation = new DatasetImpl(defaultModel);
    }

    /**
     * Create a new SageDatasetBuilder with a default RDF Graph
     * @param defaultGraph - The RDF graph used as a the default graph
     * @return A SageDatasetBuilder ready to be configured
     */
    public static SageDatasetBuilder create(Graph defaultGraph) {
        return new SageDatasetBuilder(defaultGraph);
    }

    /**
     * Create a new SageDatasetBuilder with a default Model
     * @param defaultModel - The Model used as a the default model
     * @return A SageDatasetBuilder ready to be configured
     */
    public static SageDatasetBuilder create(Model defaultModel) {
        return new SageDatasetBuilder(defaultModel);
    }

    /**
     * Create the federated dataset
     * @return A Dataset that contains all the dataset of the federation as named remote graphs
     */
    public Dataset create() {
        return federation;
    }

    /**
     * Register a Sage server in the federation
     * @param url - URL of the Sage server. It will also be used as its reference URI.
     * @return The SageDatasetBuilder, for chaining
     */
    public SageDatasetBuilder withSageServer(String url, ExecutionStats spy) {
        Model model = ModelFactory.createModelForGraph(new SageGraph(url, spy));
        federation.addNamedModel(url, model);
        return this;
    }

    /**
     * Register a named model in the federation
     * @param uri - The name of the model to add
     * @param model - Model to add
     * @return The SageDatasetBuilder, for chaining
     */
    public SageDatasetBuilder withNamedModel(String uri, Model model) {
        federation.addNamedModel(uri, model);
        return this;
    }

    /**
     * Register a named graph in the federation
     * @param uri - The name of the graph to add
     * @param graph - Graph to add
     * @return The SageDatasetBuilder, for chaining
     */
    public SageDatasetBuilder withNamedGraph(String uri, Graph graph) {
        return withNamedModel(uri, ModelFactory.createModelForGraph(graph));
    }
}
