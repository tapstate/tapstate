package io.tapstate.adapters.pdk;

import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/** Decides from identity lineage and target constraints, without choosing behavior by database name. */
public final class TargetIdentityResolver {
    private static final String OTHER_UNIQUE_CONSTRAINT = "other-unique-constraint";

    private TargetIdentityResolver() { }

    public static TargetIdentityPlan resolve(TargetTable input, TargetIdentityFacts facts) {
        List<String> logicalKey = input.fields().stream().filter(TargetField::primaryKey)
                .map(TargetField::name).toList();
        var identity = input.unwindIdentity();
        if (identity == null) return plan(TargetIdentityPlan.NOT_REQUIRED, "compatible-key", input, null, null, logicalKey);
        if (identity.parentKey().isEmpty() || logicalKey.size() <= identity.parentKey().size()
                || facts.primaryKey().isEmpty() || !identity.parentKey().containsAll(facts.primaryKey())) {
            return compatiblePlan(input, facts, logicalKey, identity.sourceIdField());
        }
        if (facts.primaryKey().size() != 1) {
            return plan(TargetIdentityPlan.ACTION_REQUIRED, "multiple-identity-fields", input, null,
                    identity.sourceIdField(), logicalKey);
        }
        String field = facts.primaryKey().getFirst();
        if (identity.sourceIdFields().size() != 1) {
            return blocked("ambiguous-source-id-field", input, field, String.join(", ", identity.sourceIdFields()), logicalKey);
        }
        String preserved = identity.sourceIdField();
        String identityProblem = identityProblem(input, facts, field, preserved);
        if (identityProblem != null) return blocked(identityProblem, input, field, preserved, logicalKey);
        UnaryOperator<String> rename = name -> name.equals(field) ? preserved : name;
        List<String> projectedKey = logicalKey.stream().map(rename).toList();
        String problem = matchingProblem(input, facts, field, preserved, projectedKey);
        if (problem != null) return blocked(problem, input, field, preserved, projectedKey);
        List<TargetField> fields = input.fields().stream().map(column -> new TargetField(
                rename.apply(column.name()), column.type(), column.primaryKey(), column.inferredType(),
                column.numericType(), column.stringType())).toList();
        List<TargetIndex> indexes = input.indexes().stream().map(index -> new TargetIndex(
                index.fields().stream().map(rename).toList(), index.unique(), index.constrainsEveryRow())).toList();
        return new TargetIdentityPlan(TargetIdentityPlan.AUTOMATIC, "generated-target-identity",
                new TargetTable(input.name(), fields, indexes), field, preserved, projectedKey, !facts.flexibleColumns(), facts.flexibleColumns());
    }

    /** Reconciles actual destination contracts while leaving unused expansion settings alone. */
    public static java.util.Map<String, TargetIdentityPlan> reconcile(java.util.Map<String, TargetIdentityPlan> plans) {
        java.util.Map<String, TargetIdentityPlan> result = new java.util.LinkedHashMap<>(plans);
        java.util.Map<String, java.util.List<String>> byTable = new java.util.LinkedHashMap<>();
        plans.forEach((stream, plan) -> byTable.computeIfAbsent(plan.target().name(), ignored -> new java.util.ArrayList<>()).add(stream));
        for (var streams : byTable.values()) {
            var automatic = streams.stream().map(plans::get).filter(TargetIdentityPlan::isAutomatic).findFirst();
            if (automatic.isEmpty()) continue;
            Set<String> key = Set.copyOf(automatic.get().writeKey());
            if (streams.stream().map(plans::get).filter(plan -> !plan.blocked())
                    .anyMatch(plan -> !key.equals(Set.copyOf(plan.writeKey())))) {
                for (String stream : streams) {
                    var plan = plans.get(stream);
                    if (!plan.blocked()) result.put(stream, blocked("target-key-conflict", plan.target(),
                            plan.sourceField(), plan.preservedField(), plan.writeKey()));
                }
            }
        }
        return result;
    }

    private static TargetIdentityPlan compatiblePlan(TargetTable input, TargetIdentityFacts facts,
            List<String> logicalKey, String preserved) {
        Set<String> writtenFields = input.fields().stream().map(TargetField::name).collect(java.util.stream.Collectors.toSet());
        if (facts.opaqueUniqueConstraints() || facts.uniqueKeys().stream().anyMatch(unique ->
                !unique.containsAll(logicalKey) && unique.stream().noneMatch(field ->
                        facts.generatedColumns().contains(field) && !writtenFields.contains(field)))) {
            return blocked(OTHER_UNIQUE_CONSTRAINT, input, null, preserved, logicalKey);
        }
        return plan(TargetIdentityPlan.NOT_REQUIRED, "compatible-key", input, null, null, logicalKey);
    }

    private static String identityProblem(TargetTable input, TargetIdentityFacts facts,
            String field, String preserved) {
        if (input.fields().stream().anyMatch(column -> column.name().equals(preserved))) {
            return "field-collision";
        }
        if (preserved.isBlank() || preserved.contains(".") || preserved.startsWith("$")
                || preserved.indexOf('\0') >= 0) {
            return "invalid-field-name";
        }
        if (!facts.generatedColumns().contains(field) || !facts.logicalKeyWrites()) {
            return "identity-generation-unavailable";
        }
        return null;
    }

    private static String matchingProblem(TargetTable input, TargetIdentityFacts facts,
            String field, String preserved, List<String> projectedKey) {
        if (facts.exists() && !facts.flexibleColumns()
                && !facts.columns().containsAll(projectedKey)) {
            return "target-columns-missing";
        }
        if (!facts.nullsCompareEqual()
                && projectedKey.stream().anyMatch(facts.nullableColumns()::contains)) {
            return "nullable-key-not-unique";
        }
        if (facts.opaqueUniqueConstraints()) {
            return OTHER_UNIQUE_CONSTRAINT;
        }
        Set<String> keySet = Set.copyOf(projectedKey);
        boolean indexed = facts.uniqueKeys().stream().anyMatch(key -> Set.copyOf(key).equals(keySet));
        if (!indexed && !facts.mayPrepareLogicalIndex()) {
            return "logical-key-index-missing";
        }
        if (facts.exists() && !facts.flexibleColumns() && !losslessKey(input, facts, field, preserved)) {
            return "key-representation-unsupported";
        }
        for (List<String> unique : facts.uniqueKeys()) {
            if (!unique.contains(field) && !unique.containsAll(projectedKey)) {
                return OTHER_UNIQUE_CONSTRAINT;
            }
        }
        return null;
    }

    private static boolean losslessKey(TargetTable input, TargetIdentityFacts facts, String field, String preserved) {
        return input.fields().stream().filter(TargetField::primaryKey).allMatch(column ->
                losslessIntegerKey(column, column.name().equals(field) ? preserved : column.name(), facts));
    }

    private static boolean losslessIntegerKey(TargetField source, String target, TargetIdentityFacts facts) {
        if (source.inferredType() != io.tapstate.core.common.TapstateType.INT64
                || !facts.signedInt64Columns().contains(target)) return false;
        var number = source.numericType();
        return number == null || (!Boolean.TRUE.equals(number.unsigned())
                && !Boolean.FALSE.equals(number.fixed()) && (number.bit() == null || number.bit() <= 64)
                && (number.scale() == null || number.scale() == 0)
                && (number.minValue() == null || number.minValue().compareTo(java.math.BigDecimal.valueOf(Long.MIN_VALUE)) >= 0)
                && (number.maxValue() == null || number.maxValue().compareTo(java.math.BigDecimal.valueOf(Long.MAX_VALUE)) <= 0));
    }

    private static TargetIdentityPlan blocked(String reason, TargetTable input, String field,
            String preserved, List<String> key) {
        return plan(TargetIdentityPlan.ACTION_REQUIRED, reason, input, field, preserved, key);
    }

    private static TargetIdentityPlan plan(String status, String reason, TargetTable target,
            String field, String preserved, List<String> key) {
        return new TargetIdentityPlan(status, reason, target, field, preserved, key);
    }
}
