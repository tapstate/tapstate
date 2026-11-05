package io.tapstate.spi.store;

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

    /** Waits for those actual owners, retrying only cleanup, under the same shutdown deadline. */
    void awaitQuiescence(long deadlineNanos);
}
