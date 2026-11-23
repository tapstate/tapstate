package io.tapstate.spi.store;

import java.util.Optional;
import java.util.function.Supplier;

/** Optional in-process deadline and ownership boundary for original claim operations. */
public interface DeadlineWorkloadClaimStore extends WorkloadClaimStore {
    /**
     * Runs the original operation under an absolute {@link System#nanoTime()} deadline.
     * A timed-out caller does not cancel an active transaction. The implementation owns its actual
     * task and session until completion, including any uncertain transaction finalization.
     */
    default <T> T withDeadline(long deadlineNanos,Supplier<T> operation) {
        return withDeadline(deadlineNanos,()->{},operation);
    }

    /** Rechecks the original thread-safe admission guard before work and commit, including retries. */
    <T> T withDeadline(long deadlineNanos,Runnable requireActive,Supplier<T> operation);

    /** Runs one original claim acquisition without taking a renewal or proof worker while it waits. */
    default <T> T withClaimAcquisition(WorkloadClaimKey key, Runnable requireActive, Supplier<T> operation) {
        requireActive.run();
        return operation.get();
    }

    /** An acquisition's queue, actual session and retries share the caller's original deadline. */
    default <T> T withClaimAcquisitionDeadline(WorkloadClaimKey key, long deadlineNanos, Runnable requireActive,
            Supplier<T> operation) {
        return withDeadline(deadlineNanos, requireActive, operation);
    }

    /** Scoped Pipeline renewal can use a separate execution lane. */
    default <T> T withRenewalDeadline(long deadlineNanos, Runnable requireActive, Supplier<T> operation) {
        return withDeadline(deadlineNanos, requireActive, operation);
    }

    /** Reserves a renewal lane for the original claim key before its callback can start. */
    default <T> T withRenewalDeadline(WorkloadClaimKey key, long deadlineNanos, Runnable requireActive, Supplier<T> operation) {
        return withRenewalDeadline(deadlineNanos, requireActive, operation);
    }

    /** Bounds a node-session renewal independently of Pipeline claim work. */
    default <T> T withNodeSessionDeadline(WorkloadClaimKey key, long deadlineNanos, Runnable requireActive,
            Supplier<T> operation) {
        return withDeadline(deadlineNanos, requireActive, operation);
    }

    /** Observes the stored node-session fence and remaining lease under the original deadline. */
    default Optional<WorkloadClaimReading> confirmNodeSession(WorkloadClaimKey key, long deadlineNanos,
            Runnable requireActive) {
        return withNodeSessionDeadline(key, deadlineNanos, requireActive, () -> read(key));
    }

    /** Waits for those actual owners, retrying only cleanup, under the same shutdown deadline. */
    void awaitQuiescence(long deadlineNanos);
}
