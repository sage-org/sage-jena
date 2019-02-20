package org.gdd.sage.federated.strategy;

import org.apache.jena.graph.Triple;
import org.gdd.sage.http.SageRemoteClient;

/**
 * An interface which encapsulates a strategy to compute relevant sources for a triple pattern during a source selection.
 * @author Thomas Minier
 */
public interface SourceSelectionStrategy {

    /**
     * Determine if a triple pattern has matching RDF triples in a RDF graph.
     * @param pattern - Triple pattern to test
     * @param graphURI - RDF graph to test
     * @param httpClient - HTTP client used to perform the ASK query
     * @return True if the pattern has matching results in this RDF graph, False otherwise
     */
    boolean isRelevant(Triple pattern, String graphURI, SageRemoteClient httpClient);
}
