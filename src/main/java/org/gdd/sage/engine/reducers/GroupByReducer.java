package org.gdd.sage.engine.reducers;

import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.data.SolutionGroup;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A reducer used to reconstruct GROUP BY results
 * @author Thomas Minier
 */
public class GroupByReducer implements Reducer {

    private Map<Binding, SolutionGroup> groups;

    public GroupByReducer() {
        groups = new HashMap<>();
    }

    @Override
    public void accumulate(SolutionGroup group) {
        Binding key = group.keyAsBindings();
        if (!groups.containsKey(key)) {
            groups.put(key, group);
        } else {
            group.forEachBindings(bindings -> groups.get(key).addBindings(bindings));
        }
    }

    @Override
    public List<SolutionGroup> getGroups() {
        return new LinkedList<>(groups.values());
    }
}
