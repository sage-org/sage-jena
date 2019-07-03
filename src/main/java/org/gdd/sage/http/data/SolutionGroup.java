package org.gdd.sage.http.data;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SolutionGroup {
    private Map<Var, Node> keys;
    private BindingHashMap bindingKey;
    private List<Binding> bindings;

    public SolutionGroup() {
        keys = new HashMap<>();
        bindings = new LinkedList<>();
        bindingKey = new BindingHashMap();
    }

    public Set<Var> getKeys() {
        return keys.keySet();
    }

    public Node getKeyValue(String key) {
        return keys.get(key);
    }

    public Binding keyAsBindings() {
        return bindingKey;
    }

    public void addKey(Var key, Node value) {
        keys.put(key, value);
        bindingKey.add(key, value);
    }

    public void addBindings(Binding bindings) {
        this.bindings.add(bindings);
    }

    public int groupSize() {
        return bindings.size();
    }

    public void forEachKey(BiConsumer<Var, Node> consumer) {
        for(Map.Entry<Var, Node> entry: keys.entrySet()) {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    public void forEachBindings(Consumer<Binding> consumer) {
        bindings.forEach(consumer::accept);
    }

    @Override
    public String toString() {
        return "SolutionGroup{" +
                "keys=" + keys +
                ", bindings=" + bindings +
                '}';
    }
}
