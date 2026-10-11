package io.tapstate.control.client;

import java.time.Duration;
import java.util.Optional;

/** Request budget for control reads, bounded writes, and live database operations. */
public enum RequestBudget {
    LIGHT,
    HEAVY,
    /** Connector initialization and discovery have no predictable read deadline. */
    CONNECTION;

    public Optional<Duration> timeout(Duration lightTimeout, Duration heavyTimeout) {
        return switch (this) {
            case LIGHT -> Optional.of(lightTimeout);
            case HEAVY -> Optional.of(heavyTimeout);
            case CONNECTION -> Optional.empty();
        };
    }
}
