package org.gdd.sage;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

public class SageQueryIterator implements QueryIterator {
    @Override
    public Binding nextBinding() {
        return null;
    }

    @Override
    public void cancel() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Binding next() {
        return null;
    }

    @Override
    public void output(IndentedWriter indentedWriter) {

    }

    @Override
    public void close() {

    }

    @Override
    public void output(IndentedWriter indentedWriter, SerializationContext serializationContext) {

    }

    @Override
    public String toString(PrefixMapping prefixMapping) {
        return null;
    }
}
