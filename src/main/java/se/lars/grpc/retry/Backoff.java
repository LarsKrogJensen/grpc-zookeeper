package se.lars.grpc.retry;

/**
 * Plugable policy for backoff when retrying operations.
 *
 * @see Backoffs
 * @see Retryer
 */
public interface Backoff {
    /**
     * Return the amount of time to wait before attempting to retry an operation.
     *
     * @param retriesAttempted Number of retries attempted so far for the operation.  Will be called
     *  with 0 for the first failed attempt.
     * @return Time to wait until the next retry
     */
    long getDelayMillis(int retriesAttempted);
}