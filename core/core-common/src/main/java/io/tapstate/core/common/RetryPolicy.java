package io.tapstate.core.common;

import java.time.Duration;
import java.util.Objects;

/** Timing and enablement shared by any operation that has a safe way to retry. */
public record RetryPolicy(boolean enabled, Duration interval, Duration maxDuration) {
    private static final Duration MAX_NANO_DURATION = Duration.ofNanos(Long.MAX_VALUE);

    public RetryPolicy {
        Objects.requireNonNull(interval, "retry interval");
        Objects.requireNonNull(maxDuration, "maximum retry duration");
        requirePositiveNanos(interval);
        requirePositiveNanos(maxDuration);
    }

    private static void requirePositiveNanos(Duration value) {
        if (value.isZero() || value.isNegative() || value.compareTo(MAX_NANO_DURATION) > 0) {
            throw new IllegalArgumentException("Retry durations must be positive and fit the monotonic clock");
        }
    }

    public RetryPolicy disabled() {
        return enabled ? new RetryPolicy(false, interval, maxDuration) : this;
    }
}
