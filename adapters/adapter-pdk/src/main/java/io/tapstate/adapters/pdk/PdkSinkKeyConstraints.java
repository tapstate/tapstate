package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.sink.TargetKeyConstraints;
import io.tapstate.spi.sink.TargetTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reads the complete target catalog without relying on the portable index projection. */
final class PdkSinkKeyConstraints {
    private PdkSinkKeyConstraints() { }

    static TargetKeyConstraints read(PdkConnector connector, TargetTable target) throws Throwable {
        if (!Set.of("mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb")
                .contains(connector.connectorId())) return TargetKeyConstraints.unknown();
        var rows = PdkIdentityIndexes.execute(connector, "aggregate", Map.of("collection", target.name(),
                "pipeline", List.of(Map.of("$listCatalog", Map.of()),
                        Map.of("$project", Map.of("_id", 0, "metadata", "$md")))));
        return mongo(rows, target);
    }

    static TargetKeyConstraints mongo(List<? extends Map<?, ?>> rows, TargetTable target) {
        if (rows.size() != 1 || !(rows.getFirst().get("metadata") instanceof Map<?, ?> metadata)
                || !(metadata.get("options") instanceof Map<?, ?> options)
                || !simpleCollation(options.get("collation"))
                || !(metadata.get("indexes") instanceof List<?> indexes) || indexes.isEmpty()) {
            return TargetKeyConstraints.unknown();
        }
        List<List<String>> keys = new ArrayList<>();
        Set<String> names = new HashSet<>();
        boolean identity = false;
        for (Object item : indexes) {
            if (!(item instanceof Map<?, ?> index) || !Boolean.TRUE.equals(index.get("ready"))
                    || !(index.get("spec") instanceof Map<?, ?> spec)
                    || !(spec.get("name") instanceof String name) || !names.add(name)) {
                return TargetKeyConstraints.unknown();
            }
            if (Boolean.TRUE.equals(spec.get("prepareUnique"))
                    || spec.containsKey("unique") && !(spec.get("unique") instanceof Boolean)) {
                return TargetKeyConstraints.unknown();
            }
            if (!name.equals("_id_") && !Boolean.TRUE.equals(spec.get("unique"))) continue;
            if (Boolean.TRUE.equals(spec.get("sparse")) || spec.containsKey("partialFilterExpression")
                    || !simpleCollation(spec.get("collation"))
                    || !(spec.get("key") instanceof Map<?, ?> fields) || fields.isEmpty()) {
                return TargetKeyConstraints.unknown();
            }
            List<String> columns = new ArrayList<>();
            for (var field : fields.entrySet()) {
                if (!(field.getKey() instanceof String column) || column.isBlank() || column.contains(".")
                        || !(field.getValue() instanceof Number direction)
                        || direction.doubleValue() != 1 && direction.doubleValue() != -1) {
                    return TargetKeyConstraints.unknown();
                }
                columns.add(column);
            }
            if (name.equals("_id_")) {
                if (!columns.equals(List.of("_id"))) return TargetKeyConstraints.unknown();
                identity = true;
            }
            keys.add(List.copyOf(columns));
        }
        if (!identity) return TargetKeyConstraints.unknown();
        boolean generated = target.fields().stream().noneMatch(field -> field.name().equals("_id"));
        var guarded = new HashSet<String>();
        target.fields().stream().filter(io.tapstate.spi.sink.TargetField::primaryKey)
                .forEach(field -> guarded.add(field.name()));
        target.indexes().stream().filter(io.tapstate.spi.sink.TargetIndex::unique)
                .forEach(index -> guarded.addAll(index.fields()));
        if (keys.stream().anyMatch(columns -> !(generated && columns.equals(List.of("_id")))
                && !guarded.containsAll(columns))) return TargetKeyConstraints.unknown();
        var fields = new java.util.HashMap<String, TapstateType>();
        for (var field : target.fields()) {
            if (guarded.contains(field.name())) {
                if (field.inferredType() != TapstateType.INT64 && field.inferredType() != TapstateType.STRING)
                    return TargetKeyConstraints.unknown();
                fields.put(field.name(), field.inferredType());
            }
        }
        return new TargetKeyConstraints(keys, true, generated, fields);
    }

    private static boolean simpleCollation(Object value) {
        return value == null || value instanceof Map<?, ?> collation && "simple".equals(collation.get("locale"));
    }
}
