package org.gdd.sage.federated.selection;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;

import java.util.Map;
import java.util.Set;

public class ServiceSourceSelection implements SourceSelection {
    @Override
    public Map<String, Set<Triple>> perform(Query query) {
        ServiceTransformer transformer = new ServiceTransformer();
        Op tree = Algebra.compile(query);
        Op tree2 = Transformer.transform(transformer, tree);
        return transformer.getSourceSelection();
    }
}
