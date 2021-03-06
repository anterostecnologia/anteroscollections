package br.com.anteros.collections.queue.file.processor;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;

import br.com.anteros.collections.queue.file.AnterosFileQueueItem;
import br.com.anteros.collections.queue.file.store.MVStoreQueue;
import br.com.anteros.collections.utils.ThreadUtil;

/**
 * Queue processor. This class is for internal use only. Please refer to FileQueue class.
 *
 */


public class QueueProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);
    private static final ThreadPoolExecutor executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() * 8, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(true),
            ThreadUtil.getFlexibleThreadFactory("filequeue-worker", false),
            new DelayRejectPolicy());
    private static final ScheduledExecutorService mvstoreCleanUPScheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),
            ThreadUtil.getFlexibleThreadFactory("mvstore-cleanup", true));
    static {
        MoreExecutors.addDelayedShutdownHook(executorService, 60L, TimeUnit.SECONDS);
        MoreExecutors.addDelayedShutdownHook(mvstoreCleanUPScheduler, 60L, TimeUnit.SECONDS);

    }

    public static void destroy() {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 60L, TimeUnit.SECONDS);
        MoreExecutors.shutdownAndAwaitTermination(mvstoreCleanUPScheduler, 60L, TimeUnit.SECONDS);
    }

    public enum RetryDelayAlgorithm { FIXED, EXPONENTIAL}

    private final ObjectMapper objectMapper;
    private final MVStoreQueue mvStoreQueue;
    private final Type type;
    private final Consumer<T> consumer;
    private final Expiration<T> expiration;
    private final Phaser restorePolled = new Phaser();
    private Optional<ScheduledFuture<?>> cleanupTaskScheduler;
    private volatile boolean doRun = true;
    private final int maxTries;
    private final int retryDelay;
    private final int persistRetryDelay;
    private final int maxRetryDelay;
    private final Path queuePath;
    private final String queueName;
    private final TimeUnit retryDelayUnit;
    private final TimeUnit persistRetryDelayUnit;
    private final RetryDelayAlgorithm retryDelayAlgorithm;


    public static class Builder {

        private     Path queuePath;
        private     String queueName;
        private     Type type;
        private     int maxTries                = 0;
        private     int retryDelay              = 1;
        private     int persistRetryDelay            = 0;
        private     int maxRetryDelay           = 1;
        private     TimeUnit retryDelayUnit = TimeUnit.SECONDS;
        private     TimeUnit persistRetryDelayUnit = TimeUnit.SECONDS;
        private     Consumer consumer;
        private     Expiration expiration;
        private     RetryDelayAlgorithm retryDelayAlgorithm =  RetryDelayAlgorithm.FIXED;
        private     ObjectMapper objectMapper = null;

        public Builder() {}

        public Builder(String queueName, Path queuePath, Type type, Consumer consumer) throws IllegalArgumentException {
            if (queueName == null) throw new IllegalArgumentException("queue name must be specified");
            if (queuePath == null) throw new IllegalArgumentException("queue path must be specified");
            if (type == null) throw new IllegalArgumentException("item type must be specified");
            if (consumer == null) throw new IllegalArgumentException("consumer must be specified");
            this.queueName = queueName;
            this.queuePath = queuePath;
            this.type = type;
            this.consumer = consumer;
        }
        /**
         * Queue path
         * @param queuePath              path to queue database
         * @return builder
         */
        public Builder queuePath(Path queuePath) { this.queuePath = queuePath; return this; }
        public Path getQueuePath() { return queuePath; }
        /**
         * Queue name
         * @param queueName              friendly name for the queue
         * @return builder
         */
        public Builder queueName(String queueName) { this.queueName = queueName; return this; }
        public String getQueueName() { return queueName; }

        /**
         * Type of queue item
         * @param type                   filequeueitem type
         * @return builder
         */
        public Builder type(Type type) { this.type = type; return this; }
        public Type getType() { return type; }

        /**
         * Maximum number of tries. Set to zero for infinite.
         * @param maxTries               maximum number of retries
         * @return builder
         */
        public Builder maxTries(int maxTries) { this.maxTries = maxTries; return this; }
        public int getMaxTries() { return maxTries; }

        /**
         * Set fixed delay in retryDelayUnit between retries
         * @param retryDelay             delay between retries
         * @return builder
         */
        public Builder retryDelay(int retryDelay) { this.retryDelay = retryDelay; return this; }
        public int getRetryDelay() { return retryDelay; }

        /**
         * Set maximum delay in retryDelayUnit between retries assuming exponential backoff enabled
         * @param maxRetryDelay            maximum delay between retries
         * @return builder
         */
        public Builder maxRetryDelay(int maxRetryDelay) { this.maxRetryDelay = maxRetryDelay; return this; }
        public int getMaxRetryDelay() { return maxRetryDelay; }

        /**
         * Set delay between retries in persistRetryDelayUnit when processing items from queue database (on disk). Items are only put on disk
         * when the in-memory-processing-queue is full
         * @param persistRetryDelay   maximum delay between retries for items on disk
         * @return builder
         */
        public Builder persistRetryDelay(int persistRetryDelay) { this.persistRetryDelay = persistRetryDelay; return this; }
        public int getPersistRetryDelay() { return persistRetryDelay; }

        /**
         * Set persistent retry delay time unit. Default is seconds.
         * @param persistRetryDelayUnit           persistent retry delay time unit
         * @return builder
         */
        public Builder persistRetryDelayUnit(TimeUnit persistRetryDelayUnit) { this.persistRetryDelayUnit = persistRetryDelayUnit; return this; }
        public TimeUnit getPersistRetryDelayUnit() { return persistRetryDelayUnit; }

        /**
         * Set retry delay time unit. Default is seconds.
         * @param retryDelayUnit           retry delay time unit
         * @return builder
         */
        public Builder retryDelayUnit(TimeUnit retryDelayUnit) { this.retryDelayUnit = retryDelayUnit; return this; }
        public TimeUnit getRetryDelayUnit() { return retryDelayUnit; }

        /**
         * Set retry delay algorithm (FIXED or EXPONENTIAL)
         * @param  retryDelayAlgorithm            set to either fixed or exponential backoff
         * @return builder
         */
        public Builder retryDelayAlgorithm(RetryDelayAlgorithm retryDelayAlgorithm) { this.retryDelayAlgorithm = retryDelayAlgorithm; return this; }
        public RetryDelayAlgorithm getRetryDelayAlgorithm() { return retryDelayAlgorithm; }

        /**
         * Set retry delay consumer
         * @param  consumer            retry delay consumer
         * @return builder
         */
        public Builder consumer(Consumer consumer) { this.consumer = consumer; return this; }
        public Consumer getConsumer() { return consumer; }

        /**
         * Set retry delay expiration
         * @param  expiration            retry delay expiration
         * @return builder
         */
        public Builder expiration(Expiration expiration) { this.expiration = expiration; return this; }
        public Expiration getExpiration() { return expiration; }

        public Builder objectMapper(ObjectMapper objectMapper){this.objectMapper = objectMapper;return this;}

        public QueueProcessor build() throws IOException, IllegalStateException, IllegalArgumentException {
            return new QueueProcessor(this);
        }
    }

    public static Builder builder(String queueName, Path queuePath, Type type, Consumer consumer) throws IllegalArgumentException {
        return new Builder(queueName, queuePath, type, consumer);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a new QueueProcessor
     * @param builder                   queue processor builder
     * @throws IllegalStateException    if the queue is not running
     * @throws IllegalArgumentException if the type cannot be serialized by jackson
     * @throws IOException              if the item could not be serialized
     */

    QueueProcessor(Builder builder) throws IOException, IllegalStateException, IllegalArgumentException {
        if (builder.queueName == null) throw new IllegalArgumentException("queue name must be specified");
        if (builder.queuePath == null) throw new IllegalArgumentException("queue path must be specified");
        if (builder.type == null) throw new IllegalArgumentException("item type must be specified");
        if (builder.consumer == null) throw new IllegalArgumentException("consumer must be specified");
        objectMapper = builder.objectMapper == null ? createObjectMapper() : builder.objectMapper;

        if (!objectMapper.canSerialize(objectMapper.constructType(builder.type).getClass())) throw new IllegalArgumentException("The given type is not serializable. it cannot be serialized by jackson");

        this.queueName      = builder.queueName;
        this.queuePath      = builder.queuePath;
        this.consumer       = builder.consumer;
        this.expiration     = builder.expiration;
        this.type           = builder.type;
        this.maxTries       = builder.maxTries;
        this.retryDelay     = builder.retryDelay;
        this.retryDelayUnit = builder.retryDelayUnit;
        this.maxRetryDelay  = builder.maxRetryDelay;
        this.retryDelayAlgorithm    = builder.retryDelayAlgorithm;
        mvStoreQueue                = new MVStoreQueue(builder.queuePath, builder.queueName);
        if (builder.persistRetryDelay<=0)
            this.persistRetryDelay  = retryDelay <= 1 ? 1 : retryDelay / 2;
        else
            this.persistRetryDelay  = builder.persistRetryDelay;
        this.persistRetryDelayUnit  = builder.persistRetryDelayUnit;
        cleanupTaskScheduler = Optional.of(mvstoreCleanUPScheduler.scheduleWithFixedDelay(new MVStoreCleaner(this), 0, persistRetryDelay, persistRetryDelayUnit));
    }

    /**
     * Get a diff between two dates
     * @param date1 the oldest date
     * @param date2 the newest date
     * @param unit the unit in which you want the diff
     * @return the diff value, in the provided unit
     */
    private static long dateDiff(Date date1, Date date2, TimeUnit unit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return unit.convert(diffInMillies,TimeUnit.MILLISECONDS);
    }


    public Path getQueueBaseDir() {
        return mvStoreQueue.getQueueDir();
    }

    public void reopen() throws IllegalStateException {
        mvStoreQueue.reopen();
    }

    /**
     * Submit item for instant processing with embedded pool. If item can't be processed instant
     * it will be queued on filesystem and processed after.
     *
     * @param item queue item
     * @throws IllegalStateException if the queue is not running
     * @throws IOException           if the item could not be serialized
     */

    public void submit(final T item) throws IllegalStateException, IOException {
        if (!doRun)
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        try {
            restorePolled.register();
            executorService.execute(new ProcessItem<>(consumer, expiration, item, this));
        } catch (RejectedExecutionException | CancellationException cancel) {
            mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
        } finally {
            restorePolled.arriveAndDeregister();
        }
    }

    public void close() {
        doRun = false;
        cleanupTaskScheduler.ifPresent(cleanupTask -> cleanupTask.cancel(true));
        restorePolled.register();
        restorePolled.arriveAndAwaitAdvance();
        mvStoreQueue.close();
    }

    public long size() {
        return mvStoreQueue.size();
    }

    private void tryItem(T item) {
            ((AnterosFileQueueItem) item).setTryDate(new Date());
            ((AnterosFileQueueItem) item).incTryCount();
          //  System.out.println("try count "+((FileQueueItem) item).getTryCount());
    }

    /**
     * Create the {@link ObjectMapper} used for serializing.
     * @return the configured {@link ObjectMapper}.
     */
    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    private boolean isNeedRetry(T item) {
        if (maxTries <= 0) return true;
        AnterosFileQueueItem queueItem = (AnterosFileQueueItem) item;
        return queueItem.getTryCount() < maxTries;
    }

    private boolean isTimeToRetry(T item) {
        switch (retryDelayAlgorithm) {
            case EXPONENTIAL:
                long tryDelay = Math.round(Math.pow(2,  ((AnterosFileQueueItem) item).getTryCount()));
                tryDelay = tryDelay > maxRetryDelay ? maxRetryDelay : tryDelay;
                tryDelay = tryDelay < retryDelay ? retryDelay : tryDelay;
                return isTimeToRetry(item, tryDelay, retryDelayUnit);
            default:  return isTimeToRetry(item, retryDelay, retryDelayUnit);
        }
    }

    private boolean isTimeToRetry(T item, long retryDelay, TimeUnit timeUnit) {
        return ((AnterosFileQueueItem) item).getTryDate() == null  || dateDiff(((AnterosFileQueueItem) item).getTryDate(), new Date(), timeUnit) > retryDelay;
    }

    private T deserialize(final byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, objectMapper.constructType(type));
        } catch (IOException e) {
            logger.error("failed deserialize object {" + Arrays.toString(data) + "}", e);
            return null;
        }
    }

    private class ProcessItem<T> implements Runnable {

        private final Consumer<T> consumer;
        private final Expiration<T> expiration;
        private final T item;
        private final QueueProcessor<T> queueProcessor;
        private boolean pushback = false;

        ProcessItem(Consumer<T> consumer, Expiration<T> expiration, T item, QueueProcessor<T> queueProcessor) {
            this.consumer = consumer;
            this.expiration = expiration;
            this.item = item;
            this.queueProcessor = queueProcessor;
        }

        private void pushBackIfNeeded() {
            if (isPushBack()) {
                try {
                    mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                } catch (Exception e1) {
                    logger.error("failed to process item {" + item.toString() + "}", e1);
                }
            }
        }

        private void flagPush() { pushback = true; }
        private boolean isPushBack() { return pushback; }

        @Override
        public void run() {
            try {
                queueProcessor.tryItem(item);
                if (consumer.consume(item)==Consumer.Result.FAIL_REQUEUE)
                    flagPush();
            } catch (InterruptedException e) {
                flagPush();
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("failed to process item {" + item.toString() + "}", e);
            } finally {
                pushBackIfNeeded();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;
            if (getClass() != o.getClass()) return false;
            ProcessItem p = (ProcessItem) o;
            return Objects.equals(item.toString(), p.item.toString());
        }

        @Override
        public int hashCode() {
            return item.toString().hashCode();
        }

    }


    private final class MVStoreCleaner implements Runnable {

        private final QueueProcessor queueProcessor;

        MVStoreCleaner(QueueProcessor queueProcessor) {
            this.queueProcessor = queueProcessor;
        }

        @Override
        public void run() {
            byte[] pushBack = null;
            if (doRun && !mvStoreQueue.isEmpty()) {
                try {
                    byte[] toDeserialize;
                    while ((toDeserialize = mvStoreQueue.poll()) != null) {
                        restorePolled.register();
                        try {
                            if (!doRun || Arrays.equals(toDeserialize, pushBack)) {
                                mvStoreQueue.push(toDeserialize);
                                break;
                            }
                            final T item = deserialize(toDeserialize);
                            if (item == null) continue;
                            if (isNeedRetry(item)) {
                                if (isTimeToRetry(item))
                                    queueProcessor.submit(item);
                                else {
                                    mvStoreQueue.push(toDeserialize);
                                    if (pushBack == null)
                                        pushBack = toDeserialize;
                                }
                            } else {
                                if (expiration != null)
                                    expiration.expire(item);
                            }
                        } catch (IllegalStateException e) {
                            logger.error("Failed to process item.", e);
                            mvStoreQueue.push(toDeserialize);
                            if (pushBack == null)
                                pushBack = toDeserialize;
                        } finally {
                            restorePolled.arriveAndDeregister();
                        }
                    }
                } catch (Exception io) {
                    logger.error("Failed to process item.", io);
                } finally {
                    mvStoreQueue.commit();
                }
            }
        }
    }

    /**
     * Get queue path
     * @return queue path
     */
    public Path getQueuePath() {return queuePath; }

    /**
     * Get queue name
     * @return queue name
     */
    public String getQueueName() { return queueName; }
    /**
     * Get retry delay consumer
     * @return retry delay consumer
     */

    public Consumer getConsumer() {return consumer; }

    /**
     * Get queue item type
     * @return type
     */
    public Type getType() { return type; }
    /**
     * Maximum number of tries. Set to zero for infinite.
     * @return maximum number of retries
     */
    public int getMaxTries() { return maxTries; }

    /**
     * Get fixed delay in retryDelayUnit between retries
     * @return delay between retries
     */
    public int getRetryDelay() { return retryDelay; }

    /**
     * Get maximum delay in retryDelayUnit between retries assuming exponential backoff enabled
     * @return maximum delay between retries
     */
    public int getMaxRetryDelay() { return maxRetryDelay; }

    /**
     * Get retry delay time unit
     * @return retry delay time unit
     */
    public TimeUnit getRetryDelayUnit() { return retryDelayUnit; }

    /**
     * Get retry delay algorithm (FIXED or EXPONENTIAL)
     * @return either fixed or exponential backoff
     */
    public RetryDelayAlgorithm getRetryDelayAlgorithm() { return retryDelayAlgorithm;}

    /**
     * Get retry delay expiration
     * @return retry delay expiration
     */
    public Expiration getExpiration() { return expiration; }

    /**
     * Get delay between processing items in queue database (on disk).
     * @return persistent retry delay
     */
    public int getPersistRetryDelay() { return persistRetryDelay; }

    /**
     * Get persistent retry delay time unit
     * @return cleanup delay time unit
     */
    public TimeUnit getPersistRetryDelayUnit() { return retryDelayUnit; }

}