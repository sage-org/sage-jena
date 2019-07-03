package org.gdd.sage.http.data;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.LinkedList;
import java.util.List;

public class QuerySolutions {
    private List<Binding> bindings;
    private List<SolutionGroup> groups;

    public QuerySolutions() {
        bindings = new LinkedList<>();
        groups = new LinkedList<>();
    }

    public List<Binding> getBindings() {
        return bindings;
    }

    public List<SolutionGroup> getGroups() {
        return groups;
    }

    public void addBindings(Binding b) {
        bindings.add(b);
    }

    public void addSolutionGroup(SolutionGroup group) {
        groups.add(group);
    }

    @Override
    public String toString() {
        return "QuerySolutions{" +
                "bindings=" + bindings +
                ", groups=" + groups +
                '}';
    }
}
