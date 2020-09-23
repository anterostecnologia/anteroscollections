package br.com.anteros.collections.queue.file;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *  AnterosFileQueueItem
 *
 *  Extend this abstract class to include the properties of a queue item. For example, an ID that refers to queued element.
 *
 *  
 */
public abstract class AnterosFileQueueItem implements Serializable {

    private AtomicInteger retryCount = new AtomicInteger(0);
    private Date tryDate;

    public AnterosFileQueueItem() {
    }

    public Date getTryDate() {
        return Objects.nonNull(tryDate) ? new Date(tryDate.getTime()) : null;
    }

    public void setTryDate(Date date) {
        this.tryDate = new Date(date.getTime());
    }

    public int getTryCount() {
        return retryCount.get();
    }

    public void setTryCount(int tryCount) {
        Preconditions.checkArgument(tryCount >= 0, "tryCount can't be less 0");
        this.retryCount.set(tryCount);
    }

    public void incTryCount() {
        this.retryCount.incrementAndGet();
    }

    public abstract String toString();

}