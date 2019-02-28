package org.gdd.sage.federated.strategy;

import org.apache.jena.graph.Triple;
import org.gdd.sage.http.SageRemoteClient;

/**
 * An interface which encapsulates a strategy to compute relevant sources for a triple pattern during a source selection.
 * @author Thomas Minier
 */
public interface SourceSelectionStrategy {

    /**
     * Get the number of matching RDF triples for a triple pattern in a RDF graph
     * @param pattern - Triple pattern to test
     * @param graphURI - RDF graph to test
     * @param httpClient - HTTP client used to perform the ASK query
     * @return The number of RDF triples matching the triple pattern in the RDF graph
     */
    int getCardinality(Triple pattern, String graphURI, SageRemoteClient httpClient);
}
