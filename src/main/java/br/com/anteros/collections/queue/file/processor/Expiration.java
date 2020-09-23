package br.com.anteros.collections.queue.file.processor;


/**
 * Implement this interface to receive notification when a queued item has expired due to maximum number of
 * retries exceeded.
 *
 */

public interface Expiration<T> {

    /**
     * Expiration notification
     *
     * @param item expired item
     */
    void expire(T item);

}