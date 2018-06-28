package org.gdd.sage.engine.optimizer;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;

public class TripleCounter extends TransformBase {
    private int nbTriples = 0;

    public int getNbTriples() {
        return nbTriples;
    }

    @Override
    public Op transform(OpBGP opBGP) {
        nbTriples += opBGP.getPattern().size();
        return super.transform(opBGP);
    }

    public static int count(Op op) {
        TripleCounter counter = new TripleCounter();
        Transformer.transform(counter, op);
        return counter.getNbTriples();
    }
}
