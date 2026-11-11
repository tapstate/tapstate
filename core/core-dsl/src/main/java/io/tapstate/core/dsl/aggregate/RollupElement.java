package io.tapstate.core.dsl.aggregate;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One live element of a level, as rendered: its key within the level, its fields with references resolved
 * and its own child levels' rollups written, and the reference paths it names but that rendered absent.
 * {@code values} is the same element as plain values - what a connector converted, taken out of the carrier
 * it travels in - which is what a rollup reads; {@code rendered} is what the document shows.
 */
public record RollupElement(List<Object> key, Map<String, Object> rendered, Map<String, Object> values,
        Set<String> missingReferences) {

    /** An element whose rendered fields are already plain values. */
    public RollupElement(List<Object> key, Map<String, Object> rendered, Set<String> missingReferences) {
        this(key, rendered, rendered, missingReferences);
    }

    public RollupElement {
        key = key == null ? List.of() : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(key));
        missingReferences = missingReferences == null ? Set.of() : Set.copyOf(missingReferences);
    }
}
