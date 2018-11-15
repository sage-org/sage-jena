package org.gdd.sage.core.analyzer;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.op.OpService;

import java.util.HashSet;
import java.util.Set;

/**
 * Collect all URIS from SERVICE clauses, and perform data localization on each BGP in each SERVICE clause
 * @author Thomas Minier
 */
public class ServiceAnalyzer extends TransformBase {
    private Set<String> uris;

    public ServiceAnalyzer() {
        super();
        uris = new HashSet<>();
    }

    public Set<String> getUris() {
        return uris;
    }

    @Override
    public Op transform(OpService opService, Op subOp) {
        uris.add(opService.getService().getURI());
        return opService;
    }
}
