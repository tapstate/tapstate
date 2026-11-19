package io.tapstate.app;

import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.model.FieldRule;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** The current names of key columns after explicit field renames. */
final class KeyProjection {
    private KeyProjection() {
    }

    static List<Set<String>> candidates(List<Set<String>> key, Map<String, FieldRule> rules,
            Predicate<String> mayContain) {
        Set<String> consumed = consumedColumns(rules);
        return key.stream().map(names -> {
            LinkedHashSet<String> next = new LinkedHashSet<>();
            rules.forEach((output, rule) -> {
                String source = rule instanceof FieldRule.Rename rename ? rename.sourceField()
                        : rule instanceof FieldRule.Computed computed
                                ? RowExpressions.movedAfterColumn(computed.celExpr()) : null;
                if (source != null && names.contains(source)) next.add(output);
            });
            for (String name : names) {
                FieldRule rule = rules.get(name);
                if (!consumed.contains(name) && !(rule instanceof FieldRule.Drop)
                        && !writesOutput(rule, mayContain)) {
                    next.add(name);
                }
            }
            if (!next.isEmpty()) return (Set<String>) next;
            // Keep a missing key as a runtime requirement when it was dropped. A replacement
            // value, however, could pass that check and incorrectly identify a different parent.
            boolean overwritten = names.stream().anyMatch(name -> writesOutput(rules.get(name), mayContain));
            return overwritten ? Set.<String>of() : names;
        }).toList();
    }

    /** Field presence through a map follows the same output, consumption and drop rules as a row. */
    static Predicate<String> projectedFields(Map<String, FieldRule> rules, Predicate<String> mayContain) {
        Set<String> consumed = consumedColumns(rules);
        return name -> {
            FieldRule rule = rules.get(name);
            return writesOutput(rule, mayContain)
                    || (!(rule instanceof FieldRule.Drop) && !consumed.contains(name) && mayContain.test(name));
        };
    }

    private static Set<String> consumedColumns(Map<String, FieldRule> rules) {
        Set<String> consumed = new LinkedHashSet<>();
        for (FieldRule rule : rules.values()) {
            if (rule instanceof FieldRule.Rename rename) consumed.add(rename.sourceField());
        }
        return consumed;
    }

    private static boolean writesOutput(FieldRule rule, Predicate<String> mayContain) {
        return switch (rule) {
            case FieldRule.Rename rename -> mayContain.test(rename.sourceField());
            case FieldRule.Computed ignored -> true;
            case FieldRule.Literal ignored -> true;
            case FieldRule.Drop ignored -> false;
            case null -> false;
        };
    }
}
