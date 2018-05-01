package org.gdd.sage.http.data;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.List;

public class QueryResults {
    public List<Binding> bindings;
    public String next;
    public SageStatistics stats;

    public QueryResults(List<Binding> bindings, String next, SageStatistics stats) {
        this.bindings = bindings;
        this.next = next;
        this.stats = stats;
    }

    public boolean hasNext() {
        return this.next != null;
    }
}
