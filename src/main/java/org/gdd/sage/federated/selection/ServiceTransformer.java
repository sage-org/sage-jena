package org.gdd.sage.federated.selection;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.HashMap;
import java.util.Map;

public class ServiceTransformer extends TransformCopy {
    private Map<BasicPattern, String> sourceSelection;

    public ServiceTransformer() {
        super();
        sourceSelection = new HashMap<>();
    }

    public Map<BasicPattern, String> getSourceSelection() {
        return sourceSelection;
    }

    @Override
    public Op transform(OpService opService, Op subOp) {
        String uri = opService.getService().getURI();
        BGPCollector visitor = new BGPCollector();
        OpWalker.walk(subOp, visitor);
        for (BasicPattern bgp: visitor.getBgpList()) {
            sourceSelection.put(bgp, uri);
            /*if (!sourceSelection.containsKey(uri)) {
                sourceSelection.put(uri, new HashSet<>());
            }
            sourceSelection.get(uri).add(bgp);*/
        }
        return subOp;
    }
}
