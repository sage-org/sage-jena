package org.gdd.sage.core.analyzer;

import org.apache.jena.sparql.expr.Expr;

import java.util.*;

public class FilterRegistry {
    private Map<Set<String>, Set<String>> content;

    public FilterRegistry() {
        content = new HashMap<>();
    }

    public void put(Set<String> variables, Expr expression) {
        FilterFormatter formatter = new FilterFormatter();
        expression.visit(formatter);
        if (!content.containsKey(variables)) {
            content.put(variables, new LinkedHashSet<>());
        }
        Set<String> prev = content.get(variables);
        prev.add(formatter.getValue());
        content.put(variables, prev);
    }

    public String getFormattedFilters(Set<String> variables) {
        List<String> filters = new LinkedList<>();
        for(Map.Entry<Set<String>, Set<String>> entry: content.entrySet()) {
            Set<String> intersection = new HashSet<>(entry.getKey());
            intersection.retainAll(variables);
            if (!intersection.isEmpty()) {
                filters.addAll(entry.getValue());
            }
        }
        Set<String> commonFilters = new LinkedHashSet<>(filters);
        if (filters.isEmpty()) {
            return "";
        } else if (commonFilters.size() == 1) {
            return filters.get(0);
        } else {
            return "(" + String.join(" && ", commonFilters.toArray(new String[0])) + ")";
        }
    }

    @Override
    public String toString() {
        return "FilterRegistry(" + content.toString() + ")";
    }
}
