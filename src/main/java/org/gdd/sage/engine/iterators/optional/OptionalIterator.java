package org.gdd.sage.engine.iterators.optional;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterConvert;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.gdd.sage.engine.iterators.base.BindingSpy;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * An universal wrapper for OPTIONAL evaluation, using the following logic:
 * 1) Store (in an internal buffer) all input bindings from the input iterator
 * 2) Evaluates the OPTIONAL clause with each input binding.
 * 3) If an input binding has results with the left-join, remove it from the local buffer.
 * 4) When all input bindings have been evaluated, forward all bindings that didn't yield results using the buffer
 */
public class OptionalIterator extends QueryIteratorBase {
    private QueryIterator internalIterator;
    private ExecutionContext executionContext;
    private Deque<Binding> seenBefore;

    /**
     * Constructor
     * @param executionContext - Query execution context
     */
    private OptionalIterator(ExecutionContext executionContext) {
        seenBefore = new LinkedList<>();
        this.executionContext = executionContext;
        internalIterator = new QueryIterNullIterator(this.executionContext);
    }

    /**
     * Create a new OptionalIterator to evaluate a left-join between an input iterator and a group pattern.
     * @param input - Input iterator
     * @param optionalClause - SPARQL group pattern from the OPTIONAL clause
     * @param executionContext - Query execution context
     * @return An OptionalIterator used to evaluates the left-join
     */
    public static OptionalIterator create(QueryIterator input, Op optionalClause, ExecutionContext executionContext) {
        BindingSpy spy = new BindingSpy();
        // intermediate iterator: between the source and the optional execution
        QueryIterator intermediateIterator = new QueryIterConvert(input, spy, executionContext);
        // wrapper iterator, that handles the OPTIONAL behaviour
        OptionalIterator optionalIterator = new OptionalIterator(executionContext);

        // catch all bindings emitted by the intermediate operator
        spy.setCallback(optionalIterator::addBinding);

        // execute the content of the optional clasue and pass the iterator to our wrapper
        optionalIterator.setInputIterator(QC.execute(optionalClause, intermediateIterator, executionContext));
        return optionalIterator;
    }

    /**
     * Set the input iterator (private, used by {@link OptionalIterator#create(QueryIterator, Op, ExecutionContext)}).
     * @param input - The new input iterator
     */
    private void setInputIterator(QueryIterator input) {
        this.internalIterator = input;
    }

    /**
     * Add bindings into the internal buffer
     * @param binding - Bindings to add
     */
    private void addBinding(Binding binding) {
        seenBefore.add(binding);
    }

    /**
     * Check if one set of bindings is the subset of another one.
     * @param left - Reference set of binding
     * @param right - Set of bindings to tests with
     * @return True if left is a subset of right, False otherwise
     */
    private boolean subsetOf(Binding left, Binding right) {
        Iterator<Var> leftVars = left.vars();
        while(leftVars.hasNext()) {
            Var currentVar = leftVars.next();
            Node associatedNode = left.get(currentVar);
            if (!right.contains(currentVar) || !associatedNode.equals(right.get(currentVar))) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean hasNextBinding() {
        return internalIterator.hasNext() || !seenBefore.isEmpty();
    }

    @Override
    protected Binding moveToNextBinding() {
        if (internalIterator.hasNext()) {
            Binding results = internalIterator.next();
            // remove the iterator from the one seen before
            for(Binding b: seenBefore) {
                if (subsetOf(b, results)) {
                    seenBefore.remove(b);
                    break;
                }
            }
            return results;
        }
        return seenBefore.pop();
    }

    @Override
    protected void closeIterator() {
        internalIterator.close();
        seenBefore.clear();
    }

    @Override
    protected void requestCancel() {
        internalIterator.cancel();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.write("OptionalIterator{");
        internalIterator.output(out, sCxt);
        out.write("}");
    }
}
