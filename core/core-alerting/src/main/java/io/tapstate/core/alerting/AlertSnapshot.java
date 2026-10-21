package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Objects;

/** Bounded current state. The caller separately persists transitions and ended episode history. */
public record AlertSnapshot(AlertRule rule, boolean enabled, Status status, Quality quality,
        long lastSequence, Instant lastObservedAt, Instant pendingSince, Instant recoverySince,
        long episodeSequence, Episode episode) {
    public enum Status { CLEAR, PENDING, FIRING, RESOLVED }
    public enum Quality { VALID, UNKNOWN, STALE, CLOCK_INVALID, DISABLED }
    public enum EndReason { RECOVERED, DISABLED, SUPERSEDED, RESOURCE_REMOVED }
    public record Acknowledgement(String principal, Instant at) {
        public Acknowledgement {
            principal = AlertValidation.text(principal, "principal", 256);
            Objects.requireNonNull(at, "at");
        }
    }
    public record Episode(long number, Instant firedAt, AlarmSeverity severity, Instant endedAt,
            Instant recoveredAt, EndReason endReason, Acknowledgement acknowledgement) {
        public Episode {
            if (number <= 0) { throw new IllegalArgumentException("episode number must be positive"); }
            Objects.requireNonNull(firedAt, "firedAt");
            Objects.requireNonNull(severity, "severity");
            if ((endedAt == null) != (endReason == null)
                    || (endReason == EndReason.RECOVERED) != (recoveredAt != null)) {
                throw new IllegalArgumentException("episode end evidence is inconsistent");
            }
        }
    }
    public AlertSnapshot { Objects.requireNonNull(rule, "rule"); Objects.requireNonNull(status, "status"); Objects.requireNonNull(quality, "quality"); }
    public static AlertSnapshot initial(AlertRule rule) {
        return new AlertSnapshot(rule, true, Status.CLEAR, Quality.UNKNOWN, -1, null, null, null, 0, null);
    }
}
