package org.gdd.sage.federated.selection;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;

import java.util.Map;
import java.util.Set;

public interface SourceSelection {
    Map<String, Set<Triple>> perform(Query query);
}
