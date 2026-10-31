package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Missing observations are a fault only when the host proves this object currently owes them. */
public final class ObservationFreshness {
    private ObservationFreshness() { }
    public static AlertSignal.Condition evaluate(boolean expected, Instant expectedSince, Instant observedAt,
            Instant now, Duration startupGrace, Duration maxAge) {
        Objects.requireNonNull(now, "now");
        AlertValidation.duration(startupGrace, "startupGrace", true, Duration.ofHours(1));
        AlertValidation.duration(maxAge, "maxAge", false, Duration.ofHours(1));
        if (!expected || expectedSince == null || expectedSince.isAfter(now)
                || observedAt != null && observedAt.isAfter(now)) { return AlertSignal.Condition.UNKNOWN; }
        if (observedAt == null || observedAt.isBefore(expectedSince)) {
            return now.isBefore(expectedSince.plus(startupGrace)) ? AlertSignal.Condition.UNKNOWN : AlertSignal.Condition.BAD;
        }
        return observedAt.plus(maxAge).isBefore(now) ? AlertSignal.Condition.BAD : AlertSignal.Condition.GOOD;
    }
}
