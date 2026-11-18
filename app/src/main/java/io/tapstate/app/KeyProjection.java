package io.tapstate.app;

import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.model.FieldRule;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The current names of key columns after explicit field renames. */
final class KeyProjection {
    private KeyProjection() {
    }

    static List<Set<String>> candidates(List<Set<String>> key, Map<String, FieldRule> rules) {
        return key.stream().map(names -> {
            LinkedHashSet<String> next = new LinkedHashSet<>();
            rules.forEach((output, rule) -> {
                String source = rule instanceof FieldRule.Rename rename ? rename.sourceField()
                        : rule instanceof FieldRule.Computed computed
                                ? RowExpressions.movedAfterColumn(computed.celExpr()) : null;
                if (source != null && names.contains(source)) next.add(output);
            });
            for (String name : names) {
                boolean consumed = rules.values().stream().anyMatch(rule ->
                        rule instanceof FieldRule.Rename rename && rename.sourceField().equals(name));
                if (!rules.containsKey(name) && !consumed) next.add(name);
            }
            if (!next.isEmpty()) return (Set<String>) next;
            boolean overwritten = names.stream().anyMatch(name -> {
                FieldRule rule = rules.get(name);
                return rule instanceof FieldRule.Computed || rule instanceof FieldRule.Literal;
            });
            return overwritten ? Set.<String>of() : names;
        }).toList();
    }
}
