package org.gdd.sage.cli;

import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;

public interface QueryExecutor {
    void execute(Model model, Query query);
}
