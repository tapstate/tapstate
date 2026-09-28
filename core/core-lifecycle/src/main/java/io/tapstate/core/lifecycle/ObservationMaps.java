package io.tapstate.core.lifecycle;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Immutable maps with stable iteration, assembled once before any telemetry sink encodes them. */
final class ObservationMaps {

    private ObservationMaps() {
    }

    static <V> Map<String, V> copyOf(Map<String, V> values) {
        if (values == null || values.isEmpty()) {
            return Map.of();
        }
        TreeMap<String, V> snapshot = new TreeMap<>();
        values.forEach((key, value) -> snapshot.put(
                Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value")));
        return Collections.unmodifiableSortedMap(snapshot);
    }
}
