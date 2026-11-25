package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

/** Evidence is independent of why further attempts stopped. It cannot authorize another send. */
public final class DeliveryEvidence {
    private DeliveryEvidence() { }
    public enum Outcome { NOT_ATTEMPTED, NOT_ACCEPTED, UNKNOWN, ACCEPTED }
    public enum RecoveryKind { NONE, WAIT, RECOVERY, UNKNOWN_SUMMARY, FAILURE_SUMMARY }
    public record Previous(Outcome outcome, boolean renderingFailed, Instant inFlightDeadline) {
        public Previous { Objects.requireNonNull(outcome, "outcome"); }
    }
    public record Recovery(RecoveryKind kind, Instant recheckAt) { }

    /** Summarizes distinct attempts/rounds; a later rejection cannot disprove another unknown request. */
    public static Outcome aggregate(Collection<Outcome> outcomes) {
        boolean unknown = false;
        boolean rejected = false;
        for (Outcome outcome : outcomes) {
            Objects.requireNonNull(outcome, "outcome");
            if (outcome == Outcome.ACCEPTED) { return Outcome.ACCEPTED; }
            unknown |= outcome == Outcome.UNKNOWN;
            rejected |= outcome == Outcome.NOT_ACCEPTED;
        }
        return unknown ? Outcome.UNKNOWN : rejected ? Outcome.NOT_ACCEPTED : Outcome.NOT_ATTEMPTED;
    }

    /** Supply every previous round for this exact episode, route, channel and target. */
    public static Recovery recovery(Collection<Previous> previous, Instant now, boolean revoked, boolean silenced) {
        Objects.requireNonNull(now, "now");
        if (revoked || silenced) { return new Recovery(RecoveryKind.NONE, null); }
        boolean accepted = false;
        boolean unknown = false;
        boolean failed = false;
        Instant nextCheck = null;
        for (Previous item : previous) {
            Instant deadline = item.inFlightDeadline();
            if (deadline != null && deadline.isAfter(now)) {
                if (nextCheck == null || deadline.isBefore(nextCheck)) { nextCheck = deadline; }
            } else if (deadline != null) {
                // A granted request without a result is unknown at its original cutoff.
                unknown = true;
            }
            accepted |= item.outcome() == Outcome.ACCEPTED;
            unknown |= item.outcome() == Outcome.UNKNOWN;
            failed |= item.renderingFailed() || item.outcome() == Outcome.NOT_ACCEPTED;
        }
        if (nextCheck != null) { return new Recovery(RecoveryKind.WAIT, nextCheck); }
        return new Recovery(accepted ? RecoveryKind.RECOVERY : unknown ? RecoveryKind.UNKNOWN_SUMMARY
                : failed ? RecoveryKind.FAILURE_SUMMARY : RecoveryKind.NONE, null);
    }
}
