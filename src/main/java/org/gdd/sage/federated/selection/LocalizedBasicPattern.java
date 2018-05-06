package org.gdd.sage.federated.selection;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.BasicPattern;

/**
 * A Basic Graph Pattern localized, i.e., tagged with a SERVICE clause
 * @deprecated
 * @author Thomas Minier
 */
public class LocalizedBasicPattern extends BasicPattern {
    private Node uri;
    private boolean silent;

    public LocalizedBasicPattern(BasicPattern other, String uri, boolean silent) {
        super(other);
        this.uri = NodeFactory.createURI(uri);
        this.silent = silent;
    }

    public LocalizedBasicPattern(BasicPattern other, String uri) {
        super(other);
        this.uri = NodeFactory.createURI(uri);
        this.silent = false;
    }

    public Node getUri() {
        return uri;
    }

    public boolean isSilent() {
        return silent;
    }
}
