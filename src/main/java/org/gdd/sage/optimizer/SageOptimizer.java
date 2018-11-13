package org.gdd.sage.optimizer;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.OptimizerMinimal;
import org.apache.jena.sparql.util.Context;

/**
 * Sage optimizer, which extends the OptimizerMinimal from Apache Jena.
 *It uses a SageTransformer to perform all rewritings
 * @author Thomas Minier
 */
public class SageOptimizer extends OptimizerMinimal {
    private Transform transformer;
    public SageOptimizer(Context context) {
        super(context);
        transformer = new SageTransformer();
    }

    @Override
    public Op rewrite(Op op) {
        return super.rewrite(Transformer.transform(transformer, op));
    }
}
