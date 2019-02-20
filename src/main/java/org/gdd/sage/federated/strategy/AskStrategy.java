package org.gdd.sage.federated.strategy;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;

/**
 * An ASK-based strategy to compute relevant sources for a triple pattern.
 * Use a ASK query to check if a triple pattern has matching RDF triples in a RDF graph.
 * @author Thomas Minier
 * @see <a href="https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#ask">SPARQL 1.1 ASK queries</a>
 */
public class AskStrategy implements SourceSelectionStrategy {
    @Override
    public boolean isRelevant(Triple pattern, String graphURI, SageRemoteClient httpClient) {
        BasicPattern bgp = new BasicPattern();
        bgp.add(pattern);
        QueryResults queryResults = httpClient.query(graphURI, bgp);
        return !queryResults.getBindings().isEmpty();
    }
}
