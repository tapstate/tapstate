package io.tapstate.spi.store;

import java.util.Objects;

/** Native end proofs obtained outside storage transactions for the exact occupied successor slot. */
public sealed interface SuccessorEnd {
    record Absent(ObservationStore.Scope scope, String submissionBootId) implements SuccessorEnd {
        public Absent {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(submissionBootId, "submissionBootId");
            if (submissionBootId.isBlank()) { throw new IllegalArgumentException("submission boot must not be blank"); }
        }
    }
    record Terminal(ObservationStore.Scope scope, StopReservation.JobIdentity job) implements SuccessorEnd {
        public Terminal { Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(job, "job"); }
    }
}
