package org.gdd.sage.optimizer;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.gdd.sage.optimizer.ops.OpFilterBGP;

public class SageTransformer extends TransformCopy {

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        subOp = Transformer.transform(this, subOp);
        // push-down filter
        // TODO
        // build OpFilterBGP if possible
        if (subOp instanceof OpBGP) {
            return new OpFilterBGP((OpBGP) subOp, opFilter);
        }
        return super.transform(opFilter, subOp);
    }
}
