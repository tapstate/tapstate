package io.tapstate.control.core;

import java.time.Duration;

/** The resolution actually used in a response; raw minute-class samples are named {@code PT1M}. */
public enum EffectiveHistoryResolution {
    PT1M(Duration.ofMinutes(1), true),
    PT5M(Duration.ofMinutes(5), false),
    PT30M(Duration.ofMinutes(30), false),
    PT1H(Duration.ofHours(1), false),
    PT3H(Duration.ofHours(3), false),
    PT6H(Duration.ofHours(6), false);

    private final Duration duration;
    private final boolean raw;

    EffectiveHistoryResolution(Duration duration, boolean raw) {
        this.duration = duration;
        this.raw = raw;
    }

    public Duration duration() {
        return duration;
    }

    public boolean raw() {
        return raw;
    }
}
