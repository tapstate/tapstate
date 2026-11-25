package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Instant;

/** Process-local completion coordinates; this receipt grants no storage authority. */
record TerminalPublication(PipelineIntentScope scope, long intentRevision, long checkpointEpoch,
        WorkloadClaimFence claim, Instant observedAt) {
    enum Status { PENDING, COMPLETE, FAILED }
    @FunctionalInterface interface Receipt {
        Status status();
        default boolean seal() { return false; }
        default void reopen() { }
        default boolean rejectedWithoutAdmission() { return false; }
        default Receipt retryRejected() { return null; }
    }
    static Receipt complete() { return new Receipt() {
        public Status status() { return Status.COMPLETE; }
        public boolean seal() { return true; }
    }; }
    static Receipt failed() { return () -> Status.FAILED; }
    static final class Admission {
        private final java.util.concurrent.atomic.AtomicInteger retained = new java.util.concurrent.atomic.AtomicInteger();
        volatile boolean failed;
        boolean enter() {
            for (;;) { int current = retained.get(); if (current < 0) return false;
                if (retained.compareAndSet(current, current + 1)) return true; }
        }
        void leave() { if (retained.decrementAndGet() < 0) throw new IllegalStateException("negative terminal work count"); }
        boolean idle() { return retained.get() <= 0; }
        boolean sealed() { return retained.get() < 0; }
        boolean seal() {
            if (failed || !retained.compareAndSet(0, -1) && !sealed()) return false;
            return !failed;
        }
        void reopen() { retained.compareAndSet(-1, 0); }
    }

    static TerminalPublication from(PipelineConvergencePass pass, long epoch, Observation observation) {
        return new TerminalPublication(pass.scope(), pass.intent().intentRevision(), epoch, pass.claim(), observation.observedAt());
    }
}
