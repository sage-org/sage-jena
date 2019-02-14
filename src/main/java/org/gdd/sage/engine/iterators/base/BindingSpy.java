package org.gdd.sage.engine.iterators.base;

import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterConvert;

/**
 * A spy, used to execute a callback over all bindings emitted by a {@link org.apache.jena.sparql.engine.QueryIterator}.
 * By default, the callback is set to nothing and must be enable with {@link BindingSpy#setCallback(Callback)}.
 */
public class BindingSpy implements QueryIterConvert.Converter {
    private Callback callback;

    /**
     * Internal interface, which specify the callback used by the iterator
     */
    public static interface Callback {
        public void execute(Binding obj);
    }

    public BindingSpy() {
        callback = null;
    }

    /**
     * Set the callback
     * @param callback - Callback
     */
    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    @Override
    public Binding convert(Binding obj) {
        if (callback != null) {
            callback.execute(obj);
        }
        return obj;
    }
}
