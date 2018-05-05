package org.gdd.sage.federated.selection;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ServiceTransformer extends TransformCopy {
    private Map<String, Set<BasicPattern>> sourceSelection;

    public ServiceTransformer() {
        super();
        sourceSelection = new HashMap<>();
    }

    public Map<String, Set<BasicPattern>> getSourceSelection() {
        return sourceSelection;
    }

    @Override
    public Op transform(OpService opService, Op subOp) {
        String uri = opService.getService().getURI();
        BGPCollector visitor = new BGPCollector();
        OpWalker.walk(subOp, visitor);
        for (BasicPattern bgp: visitor.getBgpList()) {
            if (!sourceSelection.containsKey(uri)) {
                sourceSelection.put(uri, new HashSet<>());
            }
            sourceSelection.get(uri).add(bgp);
        }
        return subOp;
    }
}
