package se.lars.grpc.retry;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Generic Retryer that encapsulates the retry mechanism for retrying operations asynchronously.
 * A Retryer is immutable and stores state for the previous retry attempt.  Each retry attempt returns
 * a new Retryer that must be used for the next retry attempt.
 */
public final class Retryer {
    private static final ScheduledExecutorService SHARED_EXECUTOR = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("retryer-%s")
            .setDaemon(true)
            .build());

    private final Backoff backoffPolicy;
    private final int maxRetrys;
    private final ScheduledExecutorService executor;
    private final Runnable beforeRetry;
    private final Future<?> future;
    private final int retryCount;

    public static Retryer createDefault() {
        return new Retryer(Backoffs.immediate(), 1, new Runnable() {
            @Override
            public void run() {
            }
        }, SHARED_EXECUTOR);
    }

    private Retryer(Backoff backoffPolicy, int maxRetrys, Runnable beforeRetry, ScheduledExecutorService executor) {
        this.backoffPolicy = backoffPolicy;
        this.maxRetrys = maxRetrys;
        this.executor = executor;
        this.beforeRetry = beforeRetry;
        this.future = null;
        this.retryCount = 0;
    }

    private Retryer(Future<?> future, Retryer other) {
        this.backoffPolicy = other.backoffPolicy;
        this.maxRetrys = other.maxRetrys;
        this.executor = other.executor;
        this.beforeRetry = other.beforeRetry;
        this.retryCount = other.maxRetrys > 0 ? other.retryCount + 1 : other.retryCount;
        this.future = future;
    }

    /**
     * Policy for determining a delay based on the number of attempts
     *
     * @param backoffPolicy The policy
     * @return The Builder
     * @see Backoffs
     */
    public Retryer backoffPolicy(Backoff backoffPolicy) {
        Preconditions.checkState(backoffPolicy != null, "Backoff policy may not be null");
        return new Retryer(backoffPolicy, maxRetrys, beforeRetry, executor);
    }

    /**
     * Maximum number of retries allowed for the call to proceed.
     *
     * @param maxRetrys Max retries or 0 for none and -1 for indefinite
     * @return The builder
     */
    public Retryer maxRetries(int maxRetrys) {
        return new Retryer(backoffPolicy, maxRetrys, beforeRetry, executor);
    }

    /**
     * Retry operations forever
     *
     * @return The builder
     */
    public Retryer retryForever() {
        return new Retryer(backoffPolicy, -1, beforeRetry, executor);
    }

    /**
     * Executor to use for scheduling delayed retries.
     *
     * @param executor The executor
     * @return The builder
     */
    public Retryer executor(ScheduledExecutorService executor) {
        Preconditions.checkState(executor != null, "Executor must not be null");
        return new Retryer(backoffPolicy, maxRetrys, beforeRetry, executor);
    }

    /**
     * Provide a runnable to invoke in between retries
     *
     * @param beforeRetry The runnable
     * @return The builder
     */
    public Retryer beforeRetry(Runnable beforeRetry) {
        Preconditions.checkState(beforeRetry == null, "Only one beforeRetry handler may be registered");
        return new Retryer(backoffPolicy, maxRetrys, beforeRetry, executor);
    }

    /**
     * Determine if a retry is allowed based on the number of attempts
     *
     * @return True if retry() is allowed
     */
    public boolean canRetry() {
        return maxRetrys < 0 || retryCount < maxRetrys;
    }

    /**
     * Cancel any scheduled retry operation
     */
    public void cancel() {
        if (this.future != null) {
            this.future.cancel(true);
        }
    }

    /**
     * Retry the operation on the provided runnable
     *
     * @param runnable The operation to retry
     * @return A new Retryer instance tracking the state of the retry operation.
     */
    Retryer retry(final Runnable runnable) {
        Preconditions.checkState(runnable != null, "Runnable must not be null");
        cancel();
        beforeRetry.run();
        long delay = backoffPolicy.getDelayMillis(retryCount);
        if (delay == 0) {
            return new Retryer(executor.submit(runnable), this);
        } else {
            return new Retryer(executor.schedule(runnable, delay, TimeUnit.MILLISECONDS), this);
        }
    }
}