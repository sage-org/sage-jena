package org.gdd.sage.core.factory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;

/**
 * Auto-build the execution environment for executing a SPARQL query with a Sage server
 */
public interface SageConfigurationFactory {
    /**
     * Auto-configure the execution environment for executing a SPARQL query with a Sage server
     */
    void configure();

    /**
     * Build the {@link Dataset} used to execute a query
     */
    void buildDataset();

    /**
     * Close the execution environment
     */
    void close();

    Query getQuery();

    Dataset getDataset();
}
