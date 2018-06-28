package org.gdd.sage.engine.optimizer;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;

public class OptionalTransformer extends TransformCopy {
    @Override
    public Op transform(OpConditional opCond, Op left, Op right) {
        return super.transform(opCond, left, right);
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        int sizeLeft = TripleCounter.count(left);
        int sizeRight = TripleCounter.count(right);
        System.out.println(left);
        System.out.println();
        System.out.println("---------");
        System.out.println(right);
        System.out.println(TripleCounter.count(right));
        return super.transform(opLeftJoin, left, right);
    }
}
