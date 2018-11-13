package org.gdd.sage.optimizer.ops;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpBase;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

public class OpFilterBGP extends OpBase {

    private OpBGP bgp;
    private OpFilter filter;

    public OpFilterBGP(OpBGP bgp, OpFilter filter) {
        this.bgp = bgp;
        this.filter = filter;
    }

    public OpBGP getBgp() {
        return bgp;
    }

    public OpFilter getFilter() {
        return filter;
    }

    @Override
    public int hashCode() {
        return bgp.hashCode() + filter.hashCode();
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        bgp.visit(opVisitor);
        filter.visit(opVisitor);
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpFilterBGP)) {
            return false;
        }
        OpFilterBGP right = (OpFilterBGP) other;
        return bgp.equalTo(right.getBgp(), labelMap) && filter.equalTo(right.getFilter(), labelMap);
    }

    @Override
    public String getName() {
        return "filter+bgp";
    }
}
