package org.gdd.sage.engine.iterators.parallel;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An iterator that performs (in parallel) the union of the results of two iterators.
 * @author Thomas Minier
 */
public class ParallelUnionIterator extends ParallelBufferedIterator {
    private QueryIterator left;
    private QueryIterator right;
    private final AtomicInteger nbTasksCompleted;

    public ParallelUnionIterator(ExecutorService threadPool, QueryIterator left, QueryIterator right) {
        super(threadPool);
        this.left = left;
        this.right = right;
        nbTasksCompleted = new AtomicInteger();
    }

    @Override
    protected void startTasks() {
        getThreadPool().submit(new ExhaustIteratorTask(left, getSharedBuffer(), nbTasksCompleted));
        getThreadPool().submit(new ExhaustIteratorTask(right, getSharedBuffer(), nbTasksCompleted));
    }

    @Override
    protected void cancelTasks() {
        left.cancel();
        right.cancel();
    }

    @Override
    protected void stopTasks() {
        left.close();
        right.close();
    }

    @Override
    protected boolean allTasksCompleted() {
        return nbTasksCompleted.get() == 2;
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        out.write("ParallelUnionIterator(");
        left.output(out, sCxt);
        out.write(", '");
        right.output(out, sCxt);
        out.write(")");
    }
}
