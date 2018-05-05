package org.gdd.sage.federated.selection;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.Map;

public class ServiceSourceSelection implements SourceSelection {
    @Override
    public Map<BasicPattern, String> perform(Query query) {
        ServiceTransformer transformer = new ServiceTransformer();
        Op tree = Algebra.compile(query);
        Transformer.transform(transformer, tree);
        return transformer.getSourceSelection();
    }
}
