package org.gdd.sage.engine.iterators.parallel;

import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A parallel version of {@link org.gdd.sage.engine.iterators.base.BlockBufferedIterator},
 * which can process several blocks in parallel using a thread pool.
 * @author Thomas Minier
 */
public abstract class ParallelBlockBufferedIterator extends QueryIteratorBase {
    protected QueryIterator source;
    private final ExecutorService threadPool;
    private final BlockingDeque<Binding> sharedBuffer;
    private final AtomicInteger activeThreads;
    private int bucketSize;
    private boolean hasStarted;
    // Maximum number of blocks processed in parallel
    private static final int MAX_RUNNING_BLOCK_THREADS = 5;

    /**
     * Constructor
     * @param source - Iterator's source
     * @param threadPool - Thread pool used to execute tasks
     * @param bucketSize - Size of the blocks
     */
    public ParallelBlockBufferedIterator(QueryIterator source, ExecutorService threadPool, int bucketSize) {
        this.source = source;
        this.threadPool = threadPool;
        this.bucketSize = bucketSize;
        sharedBuffer = new LinkedBlockingDeque<>();
        activeThreads = new AtomicInteger();
        hasStarted = false;
    }

    /**
     * Create a task used to process a block of solution mappings.
     * @param bindings - Block of solution mappings to process
     * @param output - Buffer where new results should be pushed
     * @param counter - A counter to decrement when the task has completed
     * @return The task used to process the block of solution mappings
     */
    protected abstract Runnable createTransformerTask(List<Binding> bindings, BlockingDeque<Binding> output, AtomicInteger counter);

    /**
     * Create a new block of mappings, then start a task to process it in parallel
     */
    private void startTransformerTask() {
        // create a bucket of bindings
        LinkedList<Binding> bucket = new LinkedList<>();
        while (source.hasNext() && bucket.size() < bucketSize) {
            bucket.add(source.next());
        }
        // start the task if the bucket is not empty
        if (!bucket.isEmpty()) {
            Runnable task = createTransformerTask(bucket, sharedBuffer, activeThreads);
            activeThreads.getAndIncrement();
            threadPool.execute(task);
        }
    }

    /**
     * Test if all possible tasks have been complete
     * @return True if all possible tasks have been complete, False otherwise
     */
    private boolean allTasksCompleted() {
        return !source.hasNext() && activeThreads.get() == 0;
    }

    @Override
    protected boolean hasNextBinding() {
        // warm-up phase
        if (!hasStarted) {
            hasStarted = true;
            // start some tasks to begin processing
            while (source.hasNext() && activeThreads.get() < MAX_RUNNING_BLOCK_THREADS) {
                startTransformerTask();
            }
        }
        // wait until new results are available or all active threads have completed
        while (sharedBuffer.isEmpty() && activeThreads.get() > 0) {
            synchronized (sharedBuffer) {
                try {
                    sharedBuffer.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
            // start more tasks to keep the threads busy
            while (activeThreads.get() < MAX_RUNNING_BLOCK_THREADS && source.hasNext()) {
                startTransformerTask();
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
        source.close();
    }

    @Override
    protected void requestCancel() {
        source.cancel();
    }
}
