package org.gdd.sage.federated.selection;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.Map;

public interface SourceSelection {
    Map<BasicPattern, String> perform(Query query);
}
