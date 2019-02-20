package org.gdd.sage.engine.iterators.base;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.LinkedList;
import java.util.List;

/**
 * A BufferedIterator that process block of mappings instead of single mappings.
 * @author Thomas Minier
 */
public abstract class BlockBufferedIterator extends QueryIteratorBase {
    private QueryIterator source;
    private List<Binding> bucket;
    private int bucketSize;
    private QueryIterator currentIterator;
    private ExecutionContext execCtxt;

    public BlockBufferedIterator(QueryIterator source, int size, ExecutionContext execCtxt) {
        this.source = source;
        bucket = new LinkedList<>();
        bucketSize = size;
        this.execCtxt = execCtxt;
        currentIterator = new QueryIterNullIterator(execCtxt);
    }

    public int getBucketSize() {
        return bucketSize;
    }

    protected abstract QueryIterator processBlock(List<Binding> block);

    @Override
    protected boolean hasNextBinding() {
        if (currentIterator.hasNext()) {
            return true;
        }
        while (!currentIterator.hasNext() && source.hasNext()) {
            bucket.clear();
            while (source.hasNext() && bucket.size() < bucketSize) {
                bucket.add(source.next());
            }
            currentIterator = processBlock(bucket);
        }
        return currentIterator.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        while (!currentIterator.hasNext() && source.hasNext()) {
            bucket.clear();
            while (source.hasNext() && bucket.size() < bucketSize) {
                bucket.add(source.nextBinding());
            }
            currentIterator = processBlock(bucket);
        }
        return currentIterator.nextBinding();
    }

    @Override
    protected void closeIterator() {
        source.close();
        currentIterator.close();
        bucket.clear();
    }

    @Override
    protected void requestCancel() {
        source.cancel();
        currentIterator.cancel();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        source.output(out, sCxt);
        currentIterator.output(out, sCxt);
    }
}
