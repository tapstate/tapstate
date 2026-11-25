package io.tapstate.spi.capture;

import io.tapstate.core.validation.ValidationRepair;
import io.tapstate.core.validation.ValidationTiming;
import java.util.concurrent.CompletionStage;

/**
 * Optional one-to-one normal-ingress seam. There is no default implementation or direct target write.
 * Admission rechecks current authority/retention/definition/execution, complete source reachability,
 * stable source/comparison/write identities, participation, target uniqueness, write strategy and
 * a fresh strictly settled observation. Nest, stateful paths, expansion, branching and deletion are
 * rejected. An unsupported path stays read-only, even when a source snapshot is available.
 */
public interface ValidationRepairPort {
    /**
     * Same requestId returns the original operation without another read/injection. A new repairId
     * after UNKNOWN starts fresh and never waits for or borrows the old ACK. The provider reserves
     * bounded read/write resources and establishes a source-native snapshot/CDC handoff BEFORE
     * calling the reader. If that ordering cannot be proved, it must reject before injection.
     * The guarded reader reads current source data; only its exact returned identity may be used.
     * Normal same-key ordering extends through actual target commits. Source positions, SRS and
     * normal capture ACKs are untouched; no artificial CDC hint or automatic snapshot replay.
     */
    CompletionStage<Submission> submit(ValidationRepair.Attempt attempt, ValidationTiming.Window window,
                                      SnapshotReader reader);

    @FunctionalInterface
    interface SnapshotReader {
        ValidationRepair.Snapshot read(ValidationRepair.Attempt attempt, Runnable requireActive);
    }

    /** Queue admission is separate from a real correlated writer outcome. */
    interface Submission {
        ValidationRepair.Attempt attempt();
        String submissionId();
        CompletionStage<ValidationRepair.Ack> actualWrite();
        /** Cancellation requests cleanup; it does not prove worker exit or roll back a write. */
        void cancel();
        /** Completes only after all owned submitted bodies exited or were prevented from starting. */
        CompletionStage<Void> workersExited();
    }
}
