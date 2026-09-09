package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Optional annotation block of any resource (§2). Carries labels and a free-text
 * description; never carries identity ({@code metadata.name} was abolished by F6).
 */
@Doc("Optional annotation block: labels plus a free-text description. Never carries identity.")
public record Metadata(
        @Doc("Arbitrary key/value labels attached to the resource for grouping and selection.")
        Map<String, String> labels,
        @Doc("Free-text description of the resource; never identity.")
        String description) {

    public Metadata {
        labels = labels == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(labels));
    }

    public boolean isEmpty() {
        return labels.isEmpty() && (description == null || description.isEmpty());
    }

    @Override
    public Map<String, String> labels() {
        return Objects.requireNonNullElse(labels, Map.of());
    }
}
