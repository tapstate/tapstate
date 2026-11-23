package io.tapstate.core.common;

import java.time.Duration;
import java.util.Objects;

/** Timing and enablement shared by any operation that has a safe way to retry. */
public record RetryPolicy(boolean enabled, Duration interval, Duration maxDuration) {
    public RetryPolicy {
        Objects.requireNonNull(interval, "retry interval");
        Objects.requireNonNull(maxDuration, "maximum retry duration");
        if (interval.isZero() || interval.isNegative() || maxDuration.isZero() || maxDuration.isNegative()) {
            throw new IllegalArgumentException("Retry durations must be positive");
        }
        interval.toNanos();
        maxDuration.toNanos();
    }

    public RetryPolicy disabled() {
        return enabled ? new RetryPolicy(false, interval, maxDuration) : this;
    }
}
