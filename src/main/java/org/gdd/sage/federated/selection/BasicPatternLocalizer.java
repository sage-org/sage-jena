package org.gdd.sage.federated.selection;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;

/**
 * Transform BGP operators to localize them. Each BGP is associated with a SERVICE URI and an optional SILENT modifier.
 * @deprecated
 * @author Thomas Minier
 */
public class BasicPatternLocalizer extends TransformCopy {
    private String uri;
    private boolean silent;

    public BasicPatternLocalizer(String uri, boolean silent) {
        super();
        this.uri = uri;
        this.silent = silent;
    }

    public BasicPatternLocalizer(String uri) {
        super();
        this.uri = uri;
        this.silent = false;
    }

    @Override
    public Op transform(OpBGP opBGP) {
        return new OpBGP(new LocalizedBasicPattern(opBGP.getPattern(), uri, silent));
    }
}
