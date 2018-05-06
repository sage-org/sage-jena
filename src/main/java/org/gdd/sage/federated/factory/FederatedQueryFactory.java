package org.gdd.sage.federated.factory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;

/**
 * Factory used to build the execution environment for a federated SPARQL query.
 */
public interface FederatedQueryFactory {
    /**
     * Build the federation, i.e., parse the query, localize the triple patterns and create the federation Dataset
     */
    void buildFederation();

    Query getLocalizedQuery();
    Dataset getFederationDataset();
}
