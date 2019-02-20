package org.gdd.sage.cli;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;

/**
 * Abstract query execution logic
 * @author Thomas Minier
 */
public interface QueryExecutor {
    void execute(Dataset dataset, Query query);
}
