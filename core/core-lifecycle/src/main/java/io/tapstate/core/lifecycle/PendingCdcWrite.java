package io.tapstate.core.lifecycle;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Age of an observed unconfirmed CDC write at the time its sink was measured. */
public record PendingCdcWrite(long ageMillis, Instant observedAt) {
    public static final String METRIC = "tapstate.pipeline.cdc.pending_write.age";
    private static final Duration MAX_AGE = Duration.ofSeconds(60);

    public PendingCdcWrite {
        if (ageMillis < 0) throw new IllegalArgumentException("pending write age must be non-negative");
        Objects.requireNonNull(observedAt, "observedAt");
    }

    public static boolean isCurrent(Instant observedAt, Instant now) {
        return observedAt != null && !observedAt.isAfter(now)
                && Duration.between(observedAt, now).compareTo(MAX_AGE) <= 0;
    }
}
