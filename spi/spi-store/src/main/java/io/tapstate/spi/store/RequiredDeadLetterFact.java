package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One element's current observation, with its original source positions; this is not an event archive. */
public record RequiredDeadLetterFact(String cacheIncarnation, Map<String, ChainPosition> positions, NestDeadLetterRecord record) {
    public RequiredDeadLetterFact {
        if (cacheIncarnation == null || cacheIncarnation.isBlank()) throw new IllegalArgumentException("cache incarnation must not be blank");
        Objects.requireNonNull(positions, "positions"); Objects.requireNonNull(record, "record");
        positions.forEach((chain, position) -> {
            if (chain == null || chain.isBlank() || position == null) throw new IllegalArgumentException("invalid source position");
        });
        positions = Collections.unmodifiableMap(new LinkedHashMap<>(positions));
    }
}
