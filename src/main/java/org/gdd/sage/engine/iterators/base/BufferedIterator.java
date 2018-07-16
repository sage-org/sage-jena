package org.gdd.sage.engine.iterators.base;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public abstract class BufferedIterator extends QueryIteratorBase {

    protected Deque<Binding> internalBuffer;
    private boolean warmup;

    public BufferedIterator() {
        internalBuffer = new ArrayDeque<>();
        warmup = true;
    }

    protected abstract boolean canProduceBindings();

    protected abstract List<Binding> produceBindings();

    private void fillBuffer() {
        List<Binding> bindings = produceBindings();
        internalBuffer.addAll(bindings);
    }

    @Override
    protected boolean hasNextBinding() {
        if (warmup) {
            this.fillBuffer();
            warmup = false;
        }
        if (!internalBuffer.isEmpty()) {
            return true;
        }
        return canProduceBindings();
    }

    @Override
    protected Binding moveToNextBinding() {
        // pull from internal buffer is possible, otherwise fetch more bindings from server
        if (internalBuffer.isEmpty()) {
            fillBuffer();
        }
        return internalBuffer.pollFirst();
    }

    @Override
    protected void closeIterator() {
        internalBuffer.clear();
    }

    @Override
    protected void requestCancel() {

    }

    @Override
    public void output(IndentedWriter indentedWriter, SerializationContext serializationContext) {

    }
}
