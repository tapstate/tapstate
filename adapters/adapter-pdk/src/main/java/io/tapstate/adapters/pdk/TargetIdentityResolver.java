package io.tapstate.adapters.pdk;

import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/** Decides from identity lineage and target constraints, without choosing behavior by database name. */
public final class TargetIdentityResolver {
    private TargetIdentityResolver() { }

    public static TargetIdentityPlan resolve(TargetTable input, TargetIdentityFacts facts) {
        List<String> logicalKey = input.fields().stream().filter(TargetField::primaryKey)
                .map(TargetField::name).toList();
        var identity = input.unwindIdentity();
        if (identity == null) return plan(TargetIdentityPlan.NOT_REQUIRED, "compatible-key", input, null, null, logicalKey);
        if (identity.parentKey().isEmpty() || logicalKey.size() <= identity.parentKey().size()
                || facts.primaryKey().isEmpty() || !identity.parentKey().containsAll(facts.primaryKey())) {
            Set<String> writtenFields = input.fields().stream().map(TargetField::name).collect(java.util.stream.Collectors.toSet());
            if (facts.opaqueUniqueConstraints() || facts.uniqueKeys().stream().anyMatch(unique ->
                    !unique.containsAll(logicalKey) && unique.stream().noneMatch(field ->
                            facts.generatedColumns().contains(field) && !writtenFields.contains(field)))) {
                return blocked("other-unique-constraint", input, null, identity.sourceIdField(), logicalKey);
            }
            return plan(TargetIdentityPlan.NOT_REQUIRED, "compatible-key", input, null, null, logicalKey);
        }
        if (facts.primaryKey().size() != 1) {
            return plan(TargetIdentityPlan.ACTION_REQUIRED, "multiple-identity-fields", input, null,
                    identity.sourceIdField(), logicalKey);
        }
        String field = facts.primaryKey().getFirst();
        String preserved = identity.sourceIdField();
        if (input.fields().stream().anyMatch(column -> column.name().equals(preserved))) {
            return blocked("field-collision", input, field, preserved, logicalKey);
        }
        if (preserved.isBlank() || preserved.contains(".") || preserved.startsWith("$")
                || preserved.indexOf('\0') >= 0) {
            return blocked("invalid-field-name", input, field, preserved, logicalKey);
        }
        if (!facts.generatedColumns().contains(field) || !facts.logicalKeyWrites()) {
            return blocked("identity-generation-unavailable", input, field, preserved, logicalKey);
        }
        UnaryOperator<String> rename = name -> name.equals(field) ? preserved : name;
        List<String> projectedKey = logicalKey.stream().map(rename).toList();
        if (facts.exists() && !facts.flexibleColumns()
                && !facts.columns().containsAll(projectedKey)) {
            return blocked("target-columns-missing", input, field, preserved, projectedKey);
        }
        if (!facts.nullsCompareEqual()
                && projectedKey.stream().anyMatch(facts.nullableColumns()::contains)) {
            return blocked("nullable-key-not-unique", input, field, preserved, projectedKey);
        }
        if (facts.opaqueUniqueConstraints()) {
            return blocked("other-unique-constraint", input, field, preserved, projectedKey);
        }
        Set<String> keySet = Set.copyOf(projectedKey);
        boolean indexed = facts.uniqueKeys().stream().anyMatch(key -> Set.copyOf(key).equals(keySet));
        if (facts.exists() && !indexed && !facts.mayPrepareLogicalIndex()) {
            return blocked("logical-key-index-missing", input, field, preserved, projectedKey);
        }
        if (facts.exists() && !facts.flexibleColumns() && input.fields().stream().filter(TargetField::primaryKey)
                .anyMatch(column -> !losslessIntegerKey(column, rename.apply(column.name()), facts))) {
            return blocked("key-representation-unsupported", input, field, preserved, projectedKey);
        }
        for (List<String> unique : facts.uniqueKeys()) {
            if (!unique.contains(field) && !unique.containsAll(projectedKey)) {
                return blocked("other-unique-constraint", input, field, preserved, projectedKey);
            }
        }
        List<TargetField> fields = input.fields().stream().map(column -> new TargetField(
                rename.apply(column.name()), column.type(), column.primaryKey(), column.inferredType(),
                column.numericType(), column.stringType())).toList();
        List<TargetIndex> indexes = input.indexes().stream().map(index -> new TargetIndex(
                index.fields().stream().map(rename).toList(), index.unique(), index.constrainsEveryRow())).toList();
        return new TargetIdentityPlan(TargetIdentityPlan.AUTOMATIC, "generated-target-identity",
                new TargetTable(input.name(), fields, indexes), field, preserved, projectedKey, !facts.flexibleColumns());
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
