package org.gdd.sage.cli;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;

public interface QueryExecutor {
    void execute(Dataset dataset, Query query);
}
