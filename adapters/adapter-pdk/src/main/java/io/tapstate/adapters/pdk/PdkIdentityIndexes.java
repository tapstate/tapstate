package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapstate.core.common.JsonReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Full unique-key evidence that the frozen generic index descriptor cannot represent. */
final class PdkIdentityIndexes {
    private static final Set<String> MYSQL = Set.of("mysql", "aws-rds-mysql", "aliyun-rds-mysql", "aliyun-rds-mariadb");
    private PdkIdentityIndexes() { }

    static boolean supports(String connector) {
        return connector.equals("postgres") || MYSQL.contains(connector);
    }

    record Evidence(List<List<String>> keys, boolean opaqueUniqueConstraints, Set<String> signedInt64Columns) { }

    static Evidence read(PdkConnector connector, String table) throws Throwable {
        return connector.connectorId().equals("postgres") ? postgres(connector, table) : mysql(connector, table);
    }

    private static Evidence postgres(PdkConnector connector, String table) throws Throwable {
        Object schema = connector.context().getConnectionConfig().get("schema");
        String schemaExpression = schema instanceof String name && !name.isBlank() ? literal(name, true) : "current_schema()";
        String sql = """
                SELECT i.relname AS index_name, ix.indisvalid AS is_valid, ix.indisready AS is_ready,
                  pg_get_expr(ix.indpred, ix.indrelid) AS predicate,
                  pg_get_expr(ix.indexprs, ix.indrelid) AS expressions,
                  (SELECT json_agg(a.attname)::text FROM unnest(ix.indkey) AS k(attnum)
                   JOIN pg_attribute a ON a.attrelid = ct.oid AND a.attnum = k.attnum
                   WHERE a.atttypid = 'int8'::regtype AND a.attgenerated = '' AND a.attidentity = '') AS int64_columns_json,
                  NOT EXISTS (SELECT 1 FROM unnest(ix.indcollation) AS ic(oid)
                    JOIN pg_collation co ON co.oid = ic.oid WHERE NOT co.collisdeterministic) AS deterministic,
                  (SELECT json_agg(a.attname ORDER BY k.ordinality)::text
                   FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality)
                   JOIN pg_attribute a ON a.attrelid = ct.oid AND a.attnum = k.attnum
                   WHERE k.ordinality <= ix.indnkeyatts) AS columns_json
                FROM pg_class ct JOIN pg_namespace ns ON ns.oid = ct.relnamespace
                JOIN pg_index ix ON ix.indrelid = ct.oid JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE ix.indisunique AND ns.nspname = %s AND ct.relname = %s
                """.formatted(schemaExpression, literal(table, true));
        List<List<String>> result = new ArrayList<>();
        boolean opaque = false;
        Set<String> integers = new java.util.HashSet<>();
        for (Map<?, ?> row : query(connector, sql)) {
            if (!addPostgresIndex(row, result, integers)) opaque = true;
        }
        return new Evidence(result, opaque, integers);
    }

    private static boolean addPostgresIndex(Map<?, ?> row, List<List<String>> result, Set<String> integers) {
        if (!safePostgresIndex(row)) return false;
        Object integerColumns = row.get("int64_columns_json");
        if (integerColumns instanceof String json && JsonReader.parse(json) instanceof List<?> fields) {
            fields.stream().filter(String.class::isInstance).map(String.class::cast).forEach(integers::add);
        }
        Object encoded = row.get("columns_json");
        Object decoded = encoded instanceof String json ? JsonReader.parse(json) : null;
        List<String> fields = columnNames(decoded);
        if (fields.isEmpty()) return false;
        result.add(fields);
        return true;
    }

    private static boolean safePostgresIndex(Map<?, ?> row) {
        return Boolean.TRUE.equals(row.get("is_valid")) && Boolean.TRUE.equals(row.get("is_ready"))
                && row.get("predicate") == null && row.get("expressions") == null
                && Boolean.TRUE.equals(row.get("deterministic"));
    }

    private static List<String> columnNames(Object decoded) {
        if (!(decoded instanceof List<?> fields) || fields.isEmpty()
                || fields.stream().anyMatch(field -> !(field instanceof String))) return List.of();
        return fields.stream().map(String.class::cast).toList();
    }

    private static Evidence mysql(PdkConnector connector, String table) throws Throwable {
        String sql = "SELECT s.INDEX_NAME AS index_name, s.COLUMN_NAME AS column_name, s.SUB_PART AS prefix_length, "
                + "c.COLLATION_NAME AS collation_name, c.COLUMN_TYPE AS column_type, c.GENERATION_EXPRESSION AS generated_expression FROM information_schema.statistics s "
                + "LEFT JOIN information_schema.columns c ON c.TABLE_SCHEMA = s.TABLE_SCHEMA "
                + "AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME "
                + "WHERE s.NON_UNIQUE = 0 AND s.TABLE_SCHEMA = DATABASE() "
                + "AND s.TABLE_NAME = " + literal(table, false) + " ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX";
        Map<String, List<String>> indexes = new LinkedHashMap<>();
        Set<String> integers = new java.util.HashSet<>();
        Set<String> unsupported = new java.util.HashSet<>();
        for (Map<?, ?> row : query(connector, sql)) {
            String name = String.valueOf(row.get("index_name"));
            if (!(row.get("column_name") instanceof String field) || row.get("prefix_length") != null
                    || row.get("collation_name") != null) {
                unsupported.add(name);
            } else {
                indexes.computeIfAbsent(name, ignored -> new ArrayList<>()).add(field);
                if ("".equals(row.get("generated_expression")) && row.get("column_type") instanceof String type && type.toLowerCase(java.util.Locale.ROOT)
                        .matches("bigint(?:\\(\\d+\\))?")) integers.add(field);
            }
        }
        unsupported.forEach(indexes::remove);
        return new Evidence(new ArrayList<>(indexes.values()), !unsupported.isEmpty(), integers);
    }

    static Evidence mongo(PdkConnector connector, String table) throws Throwable {
        List<List<String>> keys = new ArrayList<>();
        boolean opaque = false;
        var rows = execute(connector, "aggregate", Map.of("collection", table,
                "pipeline", List.of(Map.of("$indexStats", Map.of())), "batchSize", 1000));
        if (rows.isEmpty()) throw new IllegalStateException("target index inspection returned no index specs");
        for (Map<?, ?> row : rows) {
            if (!(row.get("spec") instanceof Map<?, ?> spec) || !(spec.get("key") instanceof Map<?, ?> fields)) {
                throw new IllegalStateException("target index inspection returned an incomplete index spec");
            }
            if (Boolean.TRUE.equals(spec.get("unique")) || "_id_".equals(spec.get("name"))) {
                if (safeMongoIndex(spec, fields)) keys.add(fields.keySet().stream().map(String.class::cast).toList());
                else opaque = true;
            }
        }
        return new Evidence(keys, opaque, Set.of());
    }

    private static boolean safeMongoIndex(Map<?, ?> spec, Map<?, ?> fields) {
        return !Boolean.TRUE.equals(spec.get("sparse")) && !spec.containsKey("partialFilterExpression")
                && !spec.containsKey("collation") && !fields.isEmpty()
                && fields.keySet().stream().allMatch(String.class::isInstance)
                && fields.values().stream().allMatch(value -> value instanceof Number number
                        && (number.intValue() == 1 || number.intValue() == -1));
    }

    private static List<Map<Object, Object>> query(PdkConnector connector, String sql) throws Throwable {
        return execute(connector, "executeQuery", Map.of("sql", sql, "batchSize", 1000));
    }

    static List<Map<Object, Object>> execute(PdkConnector connector, String command, Map<String, Object> params) throws Throwable {
        var execute = connector.functions().getExecuteCommandFunction();
        if (execute == null) throw new IllegalStateException("exact target index inspection is unavailable");
        List<Map<Object, Object>> rows = new ArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean answered = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        execute.execute(connector.context(), TapExecuteCommand.create().command(command)
                .params(new LinkedHashMap<>(params)), result -> {
                    if (result == null) return;
                    if (result.getError() != null) failure.set(result.getError());
                    else if (result.getResult() instanceof List<?> batch) {
                        answered.set(true);
                        for (Object row : batch) {
                            if (!(row instanceof Map<?, ?> values)) {
                                failure.set(new IllegalStateException("target index inspection returned a non-row value"));
                                break;
                            }
                            rows.add(new LinkedHashMap<>(values));
                        }
                    }
                });
        if (failure.get() != null) throw failure.get();
        if (!answered.get()) throw new IllegalStateException("target index inspection returned no result");
        return rows;
    }

    private static String literal(String value, boolean postgres) {
        String hex = java.util.HexFormat.of().formatHex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return postgres ? "convert_from(decode('" + hex + "', 'hex'), 'UTF8')"
                : "CONVERT(X'" + hex + "' USING utf8mb4)";
    }
}
