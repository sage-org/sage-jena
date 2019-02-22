package org.gdd.sage.engine.iterators.parallel;

import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * An abstraction of a buffered iterator capable of producing bindings in parallel.
 * A thread pool is responsible for executing tasks that fills up the iterator internal buffer.
 * @author Thomas Minier
 */
public abstract class ParallelBufferedIterator extends QueryIteratorBase {
    private final ExecutorService threadPool;
    private final BlockingDeque<Binding> sharedBuffer;
    private boolean hasStarted;

    /**
     * Constructor
     * @param threadPool - Thread pool used to execute tasks
     */
    public ParallelBufferedIterator(ExecutorService threadPool) {
        this.threadPool = threadPool;
        sharedBuffer = new LinkedBlockingDeque<>();
        hasStarted = false;
    }

    /**
     * Get the thread pool used by the iterator
     * @return The thread pool used by the iterator
     */
    protected ExecutorService getThreadPool() {
        return threadPool;
    }

    /**
     * Get the iterator's internal buffer (used to store new bindings)
     * @return The iterator's internal buffer
     */
    protected BlockingDeque<Binding> getSharedBuffer() {
        return sharedBuffer;
    }

    /**
     * Start all tasks used to produce results
     */
    protected abstract void startTasks();

    /**
     * Cancel all tasks used to produce results
     */
    protected abstract void cancelTasks();

    /**
     * Stop all tasks used to produce results
     */
    protected abstract void stopTasks();

    /**
     * Test if all tasks have been completed
     * @return True if all tasks have been completed, False otherwise
     */
    protected abstract boolean allTasksCompleted();

    @Override
    protected boolean hasNextBinding() {
        if (!hasStarted) {
            startTasks();
            hasStarted = true;
        }
        while (sharedBuffer.isEmpty() && !allTasksCompleted()) {
            synchronized (sharedBuffer) {
                try {
                    sharedBuffer.wait();
                } catch (InterruptedException e) {
                    return false;
                }
            }
        }
        return !sharedBuffer.isEmpty() || !allTasksCompleted();
    }

    @Override
    protected Binding moveToNextBinding() {
        return sharedBuffer.pollFirst();
    }

    @Override
    protected void closeIterator() {
        stopTasks();
    }

    @Override
    protected void requestCancel() {
        cancelTasks();
    }
}
