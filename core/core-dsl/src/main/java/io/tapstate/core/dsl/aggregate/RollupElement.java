package io.tapstate.core.dsl.aggregate;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One live element of a level, as rendered: its key within the level, its fields with references resolved
 * and its own child levels' rollups written, and the reference paths it names but that rendered absent.
 */
public record RollupElement(List<Object> key, Map<String, Object> rendered, Set<String> missingReferences) {

    public RollupElement {
        key = key == null ? List.of() : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(key));
        missingReferences = missingReferences == null ? Set.of() : Set.copyOf(missingReferences);
    }
}
