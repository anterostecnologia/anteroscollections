package br.com.anteros.collections.queue.file.processor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DelayRejectPolicy
 *
 */

class DelayRejectPolicy implements RejectedExecutionHandler {
    private final long timeOut;
    private final TimeUnit timeUnit;


    /**
     * Creates a {@code DelayRejectPolicy} for the given executor.
     */
    public DelayRejectPolicy() {
        this.timeOut = 1L;
        this.timeUnit = TimeUnit.SECONDS;
    }

    public DelayRejectPolicy(long timeOut, TimeUnit timeUnit) {
        this.timeOut = timeOut;
        this.timeUnit = timeUnit;
    }

    /**
     * Obtains and ignores the next task that the executor
     * would otherwise execute, if one is immediately available,
     * and then retries execution of task r, unless the executor
     * is shut down, in which case task r is instead discarded.
     *
     * @param r the runnable task requested to be executed
     * @param e the executor attempting to execute this task
     */
    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        if (!e.isShutdown()) {
            try {
                if (!e.getQueue().offer(r, timeOut, timeUnit))
                    throw new RejectedExecutionException("Task " + r.toString() +
                            " need save to disk.");

            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Task " + r.toString() +
                        " rejected from " +
                        e.toString() + " due to interruption.");
            }
        } else
            throw new RejectedExecutionException("Task " + r.toString() +
                    " rejected from " +
                    e.toString() + " shutdown.");
    }
}
