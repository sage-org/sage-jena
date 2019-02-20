package org.gdd.sage.engine.iterators.base;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 * Utility iterator: makes the union of two other iterators
 * @author Thomas Minier
 */
public class UnionIterator extends QueryIteratorBase {
    private QueryIterator left;
    private QueryIterator right;

    public UnionIterator(QueryIterator left, QueryIterator right) {
        this.left = left;
        this.right = right;
    }

    @Override
    protected boolean hasNextBinding() {
        return left.hasNext() || right.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        if (left.hasNext()) {
            return left.next();
        }
        return right.next();
    }

    @Override
    protected void closeIterator() {
        left.close();
        right.close();
    }

    @Override
    protected void requestCancel() {
        left.cancel();
        right.cancel();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {

    }
}
