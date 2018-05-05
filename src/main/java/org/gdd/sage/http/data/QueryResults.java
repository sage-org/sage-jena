package org.gdd.sage.http.data;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.List;
import java.util.Optional;

public class QueryResults {
    private List<Binding> bindings;
    private Optional<String> next;
    private SageStatistics stats;
    private String error;
    private boolean hasError;

    public QueryResults(List<Binding> bindings, String next, SageStatistics stats) {
        this.bindings = bindings;
        if (next == null) {
            this.next = Optional.empty();
        } else {
            this.next = Optional.of(next);
        }
        this.stats = stats;
        this.error = "No error during query evaluation";
        this.hasError = false;
    }


    private QueryResults(String error) {
        this.error = error;
        this.hasError = true;
    }

    public static QueryResults withError(String error) {
        return new QueryResults(error);
    }

    public List<Binding> getBindings() {
        return bindings;
    }

    public Optional<String> getNext() {
        return next;
    }

    public SageStatistics getStats() {
        return stats;
    }

    public String getError() {
        return error;
    }

    public boolean hasError() {
        return this.hasError;
    }

    public boolean hasNext() {
        return this.next.isPresent();
    }
}
