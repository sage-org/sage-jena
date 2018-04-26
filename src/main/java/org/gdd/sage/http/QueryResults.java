package org.gdd.sage.http;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.List;

public class QueryResults {
    public List<Binding> bindings;
    public String next;

    public QueryResults(List<Binding> bindings, String next) {
        this.bindings = bindings;
        this.next = next;
    }

    public boolean hasNext() {
        return this.next != null;
    }
}
