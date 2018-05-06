package org.gdd.sage.federated.factory;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;

public interface FederatedQueryFactory {
    void buildFederation();
    Op getLocalizedQuery();
    DatasetGraph getFederationDataset();
}
