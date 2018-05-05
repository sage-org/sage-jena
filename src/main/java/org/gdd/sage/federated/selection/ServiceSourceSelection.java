package org.gdd.sage.federated.selection;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.Map;
import java.util.Set;

public class ServiceSourceSelection implements SourceSelection {
    @Override
    public Map<String, Set<BasicPattern>> perform(Query query) {
        ServiceTransformer transformer = new ServiceTransformer();
        Op tree = Algebra.compile(query);
        Op tree2 = Transformer.transform(transformer, tree);
        return transformer.getSourceSelection();
    }
}
