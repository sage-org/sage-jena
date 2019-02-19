package org.gdd.sage.core.factory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.gdd.sage.core.analyzer.FilterRegistry;

/**
 * Factory used to build the execution environment for executing a SPARQL query with a Sage server
 */
public interface SageConfigurationFactory {
    /**
     * Build the federation, i.e., parse the query, localize the triple patterns and create the federation Dataset
     */
    void buildDataset();

    Query getQuery();
    Dataset getDataset();
    FilterRegistry getFilters();
}
