package org.gdd.sage.http.results;

import org.gdd.sage.http.data.SageStatistics;

/**
 * Results obtained after executing a SPARQL query against a Sage server
 * @author Thomas Minier
 */
public interface SageQueryResults {
    SageStatistics getStats();

    String getError();

    boolean hasError();
}
