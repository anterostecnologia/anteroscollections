package br.com.anteros.collections.queue.file.processor;

/**
 * Implement this interface to consume items on the queue. File Queue will call the consume method each time an item
 * is available for processing.
 *
 */
public interface Consumer<T> {

    /**
     * Result of consumption.
     *
     **/

    enum Result {

        /**
         * Processing of item was successful. Do not requeue.
         **/

        SUCCESS, /* process was successful */

        /**
         * Processing of item failed, however retry later.
         **/

        FAIL_REQUEUE,  /* process failed, but must be requeued */

        /**
         * Processing of item failed permanently. No retry.
         **/

        FAIL_NOQUEUE /* process failed, don't requeue */
    }

    /**
     * Consume the given item. This callback is called by FileQueue when an item is available for processing.
     *
     * @param item to handle.
     * @return {@code SUCCESS} if the item was processed successfully and shall be removed from the filequeue.
     * @throws InterruptedException if thread was interrupted due to shutdown
     */

    Result consume(T item) throws InterruptedException;

}