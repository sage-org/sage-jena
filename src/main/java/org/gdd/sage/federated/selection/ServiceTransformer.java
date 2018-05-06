package org.gdd.sage.federated.selection;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpService;

import java.util.HashSet;
import java.util.Set;

/**
 * Collect all URIS from SERVICE clauses, and perform data localization on each BGP in each SERVICE clause
 * @author Thomas Minier
 */
public class ServiceTransformer extends TransformCopy {
    private Set<String> uris;

    public ServiceTransformer() {
        super();
        uris = new HashSet<>();
    }

    public Set<String> getUris() {
        return uris;
    }

    @Override
    public Op transform(OpService opService, Op subOp) {
        String uri = opService.getService().getURI();
        // TODO add SILENT support
        boolean silent = opService.getSilent();
        uris.add(uri);
        // BasicPatternLocalizer bgpLocalizer = new BasicPatternLocalizer(uri, silent);
        // Transform each service clause in graph clause, to rely on named sage Dataset next
        return new OpGraph(NodeFactory.createURI(uri), subOp);
    }
}
