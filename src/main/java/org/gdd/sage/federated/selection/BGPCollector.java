package org.gdd.sage.federated.selection;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;

import java.util.ArrayList;
import java.util.List;

public class BGPCollector extends OpVisitorBase {
    private List<Triple> tripleList = new ArrayList<>();

    @Override
    public void visit(OpBGP opBGP) {
        opBGP.getPattern().forEach(tripleList::add);
    }

    public List<Triple> getTripleList() {
        return tripleList;
    }
}
