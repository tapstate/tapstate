package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Objects;

/** A provider conclusion, not proof of durable acceptance or permission to send. */
public record AlertSignal(AlarmKey key, long revision, long generation, String producerIncarnation,
        long sequence, Instant observedAt, Condition condition, AlarmSeverity severity) {
    public enum Condition { BAD, GOOD, UNKNOWN }
    public AlertSignal {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(observedAt, "observedAt");
        Objects.requireNonNull(condition, "condition");
        Objects.requireNonNull(severity, "severity");
        producerIncarnation = AlertValidation.text(producerIncarnation, "producerIncarnation", 256);
        AlertValidation.require(sequence >= 0, "sequence", "must-be-nonnegative");
    }
}
