package io.tapstate.app;

import io.tapstate.core.model.FieldRule;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** The current names of key columns after explicit field renames. */
final class KeyProjection {
    private KeyProjection() {
    }

    /**
     * Follows renames without discarding required columns. A dropped key keeps its required name,
     * so the expansion's before-image check still refuses its absence; removing it here would
     * silently weaken the parent identity. This does not infer identity from computed values.
     */
    static List<String> renamed(List<String> key, Map<String, FieldRule> rules) {
        Map<String, String> renamedTo = new LinkedHashMap<>();
        rules.forEach((output, rule) -> {
            if (rule instanceof FieldRule.Rename rename) {
                renamedTo.put(rename.sourceField(), output);
            }
        });
        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (String column : key) {
            names.add(renamedTo.getOrDefault(column, column));
        }
        return List.copyOf(names);
    }
}
