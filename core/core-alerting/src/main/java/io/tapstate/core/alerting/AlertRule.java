package io.tapstate.core.alerting;

import java.time.Duration;
import java.util.Objects;

/** One bounded evaluator binding. Replacing a producer requires a new evaluation generation. */
public record AlertRule(AlarmKey key, long revision, long generation, String producerIncarnation,
        Duration triggerFor, Duration resolveFor, Duration maxAge, Duration maxGap) {
    public AlertRule {
        Objects.requireNonNull(key, "key");
        AlertValidation.require(revision > 0, "revision", "must-be-positive");
        AlertValidation.require(generation > 0, "generation", "must-be-positive");
        producerIncarnation = AlertValidation.text(producerIncarnation, "producerIncarnation", 256);
        triggerFor = AlertValidation.duration(triggerFor, "triggerFor", true, Duration.ofDays(1));
        resolveFor = AlertValidation.duration(resolveFor, "resolveFor", true, Duration.ofDays(1));
        maxAge = AlertValidation.duration(maxAge, "maxAge", false, Duration.ofHours(1));
        maxGap = AlertValidation.duration(maxGap, "maxGap", false, maxAge);
    }
}
