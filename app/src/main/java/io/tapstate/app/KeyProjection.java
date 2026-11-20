package io.tapstate.app;

import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.model.FieldRule;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

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

    record GuardedCandidates(List<Set<String>> names, Map<String, String> guardedRenames) {}

    /** A common alias must exist for each ordered key component. */
    static Optional<List<Set<String>>> sharedCandidates(List<Set<String>> left, List<Set<String>> right,
            Predicate<String> available) {
        if (left.size() != right.size()) return Optional.empty();
        List<Set<String>> shared = new ArrayList<>(left.size());
        for (int component = 0; component < left.size(); component++) {
            Set<String> common = new LinkedHashSet<>(left.get(component));
            common.retainAll(right.get(component));
            common.removeIf(available.negate());
            if (common.isEmpty()) return Optional.empty();
            shared.add(common);
        }
        return Optional.of(List.copyOf(shared));
    }

    /** Prefer aliases that survive every possible rename, then guard any required fallback. */
    static GuardedCandidates guardedCandidates(List<Set<String>> key, Map<String, FieldRule> rules,
            Predicate<String> knownFields, Predicate<String> possibleFields) {
        List<Set<String>> stable = candidates(key, rules, possibleFields);
        if (stable.stream().noneMatch(Set::isEmpty)) return new GuardedCandidates(stable, Map.of());

        List<Set<String>> fallback = candidates(key, rules, knownFields);
        List<Set<String>> names = new ArrayList<>(stable);
        Map<String, String> renames = new LinkedHashMap<>();
        for (int component = 0; component < stable.size(); component++) {
            if (stable.get(component).isEmpty()) {
                Set<String> retained = fallback.get(component);
                names.set(component, retained);
                recordGuardedRenames(retained, rules, knownFields, possibleFields, renames);
            }
        }
        return new GuardedCandidates(List.copyOf(names), Map.copyOf(renames));
    }

    private static void recordGuardedRenames(Set<String> retained, Map<String, FieldRule> rules,
            Predicate<String> knownFields, Predicate<String> possibleFields, Map<String, String> renames) {
        for (String output : retained) {
            if (rules.get(output) instanceof FieldRule.Rename rename
                    && !knownFields.test(rename.sourceField()) && possibleFields.test(rename.sourceField())) {
                renames.put(output, rename.sourceField());
            }
        }
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
