package org.gdd.sage.federated;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetImpl;
import org.gdd.sage.http.ExecutionStats;
import org.gdd.sage.model.SageGraph;

/**
 * Builder used to create a new federated Dataset of Sage server
 * @author Thomas Minier
 */
public class FederatedDatasetBuilder {
    private Dataset federation;

    private FederatedDatasetBuilder(Graph defaultGraph) {
        federation = new DatasetImpl(ModelFactory.createModelForGraph(defaultGraph));
    }

    private FederatedDatasetBuilder(Model defaultModel) {
        federation = new DatasetImpl(defaultModel);
    }

    /**
     * Create a new FederatedDatasetBuilder with a default RDF Graph
     * @param defaultGraph - The RDF graph used as a the default graph
     * @return A FederatedDatasetBuilder ready to be configured
     */
    public static FederatedDatasetBuilder create(Graph defaultGraph) {
        return new FederatedDatasetBuilder(defaultGraph);
    }

    /**
     * Create a new FederatedDatasetBuilder with a default Model
     * @param defaultModel - The Model used as a the default model
     * @return A FederatedDatasetBuilder ready to be configured
     */
    public static FederatedDatasetBuilder create(Model defaultModel) {
        return new FederatedDatasetBuilder(defaultModel);
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
     * @return The FederatedDatasetBuilder, for chaining
     */
    public FederatedDatasetBuilder withSageServer(String url, ExecutionStats spy) {
        Model model = ModelFactory.createModelForGraph(new SageGraph(url, spy));
        federation.addNamedModel(url, model);
        return this;
    }

    /**
     * Register a named model in the federation
     * @param uri - The name of the model to add
     * @param model - Model to add
     * @return The FederatedDatasetBuilder, for chaining
     */
    public FederatedDatasetBuilder withNamedModel(String uri, Model model) {
        federation.addNamedModel(uri, model);
        return this;
    }

    /**
     * Register a named graph in the federation
     * @param uri - The name of the graph to add
     * @param graph - Graph to add
     * @return The FederatedDatasetBuilder, for chaining
     */
    public FederatedDatasetBuilder withNamedGraph(String uri, Graph graph) {
        return withNamedModel(uri, ModelFactory.createModelForGraph(graph));
    }
}
