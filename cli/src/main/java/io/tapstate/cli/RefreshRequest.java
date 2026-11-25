package io.tapstate.cli;

import java.util.Objects;

/** One refresh invocation with the identity and cancellation state needed to fence late completion. */
record RefreshRequest(
        long contextGeneration,
        long requestSequence,
        CancellationToken cancellationToken,
        Work work) {

    RefreshRequest {
        if (contextGeneration < 0) {
            throw new IllegalArgumentException("Context generation must not be negative");
        }
        if (requestSequence <= 0) {
            throw new IllegalArgumentException("Request sequence must be positive");
        }
        Objects.requireNonNull(cancellationToken, "cancellationToken");
        Objects.requireNonNull(work, "work");
    }

    /** Produces one typed outcome without touching terminal or UI state. */
    @FunctionalInterface
    interface Work {
        RefreshResult.Outcome load(
                long contextGeneration,
                long requestSequence,
                CancellationToken cancellationToken) throws Exception;
    }

    /** Cooperative cancellation shared with the refresh operation. */
    static final class CancellationToken {
        private boolean cancelled;

        synchronized boolean isCancelled() {
            return cancelled;
        }

        synchronized void throwIfCancelled() throws InterruptedException {
            if (cancelled) {
                throw new InterruptedException("Refresh was cancelled");
            }
        }

        /** Runs one short mutation while preventing cancellation from returning halfway through it. */
        synchronized boolean mutateIfActive(Runnable mutation) {
            Objects.requireNonNull(mutation, "mutation");
            if (cancelled) {
                return false;
            }
            mutation.run();
            return true;
        }

        synchronized void cancel() {
            cancelled = true;
        }
    }
}
