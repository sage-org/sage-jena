package org.gdd.sage.federated.selection;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.Map;
import java.util.Set;

public interface SourceSelection {
    Map<String, Set<BasicPattern>> perform(Query query);
}
