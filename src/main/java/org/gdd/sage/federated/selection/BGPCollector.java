package org.gdd.sage.federated.selection;

import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.ArrayList;
import java.util.List;

public class BGPCollector extends OpVisitorBase {
    private List<BasicPattern> bgpList = new ArrayList<>();

    @Override
    public void visit(OpBGP opBGP) {
        bgpList.add(opBGP.getPattern());
    }

    public List<BasicPattern> getBgpList() {
        return bgpList;
    }
}
