package io.tapstate.spi.store;

import java.util.Objects;

/** A consumer holds one stable Pipeline state identity across ordinary execution restarts. */
public record SrsConsumerContext(SrsConsumerKey key, long stateGeneration) {
    public SrsConsumerContext {
        Objects.requireNonNull(key, "key");
        if (stateGeneration < 1 || stateGeneration == Long.MAX_VALUE) throw new IllegalArgumentException("invalid consumer state generation");
    }
}
