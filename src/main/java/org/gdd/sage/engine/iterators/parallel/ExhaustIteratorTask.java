package org.gdd.sage.engine.iterators.parallel;

import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Tasks that read all results from an iterators and insert them into a shared buffer.
 * @author Thomas Minier
 */
public class ExhaustIteratorTask implements Runnable {
    private QueryIterator source;
    private final BlockingDeque<Binding> buffer;
    private final AtomicInteger activeThreadsCounter;

    public ExhaustIteratorTask(QueryIterator source, BlockingDeque<Binding> buffer, AtomicInteger activeThreadsCounter) {
        this.source = source;
        this.buffer = buffer;
        this.activeThreadsCounter = activeThreadsCounter;
    }

    @Override
    public void run() {
        while (source.hasNext()) {
            synchronized (buffer) {
                buffer.add(source.next());
                buffer.notifyAll();
            }
        }
        activeThreadsCounter.getAndDecrement();
        synchronized (buffer) {
            buffer.notifyAll();
        }
    }
}
