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

    record Evidence(List<List<String>> keys, boolean opaqueUniqueConstraints) { }

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
        for (Map<?, ?> row : query(connector, sql)) {
            if (!Boolean.TRUE.equals(row.get("is_valid")) || !Boolean.TRUE.equals(row.get("is_ready"))
                    || row.get("predicate") != null || row.get("expressions") != null
                    || !Boolean.TRUE.equals(row.get("deterministic"))) {
                opaque = true;
                continue;
            }
            Object encoded = row.get("columns_json");
            Object decoded = encoded instanceof String json ? JsonReader.parse(json) : null;
            if (!(decoded instanceof List<?> fields) || fields.isEmpty()
                    || fields.stream().anyMatch(field -> !(field instanceof String))) {
                opaque = true;
                continue;
            }
            result.add(fields.stream().map(String.class::cast).toList());
        }
        return new Evidence(result, opaque);
    }

    private static Evidence mysql(PdkConnector connector, String table) throws Throwable {
        String sql = "SELECT s.INDEX_NAME AS index_name, s.COLUMN_NAME AS column_name, s.SUB_PART AS prefix_length, "
                + "c.COLLATION_NAME AS collation_name FROM information_schema.statistics s "
                + "LEFT JOIN information_schema.columns c ON c.TABLE_SCHEMA = s.TABLE_SCHEMA "
                + "AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME "
                + "WHERE s.NON_UNIQUE = 0 AND s.TABLE_SCHEMA = DATABASE() "
                + "AND s.TABLE_NAME = " + literal(table, false) + " ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX";
        Map<String, List<String>> indexes = new LinkedHashMap<>();
        Set<String> unsupported = new java.util.HashSet<>();
        for (Map<?, ?> row : query(connector, sql)) {
            String name = String.valueOf(row.get("index_name"));
            if (!(row.get("column_name") instanceof String field) || row.get("prefix_length") != null
                    || row.get("collation_name") != null) {
                unsupported.add(name);
            } else {
                indexes.computeIfAbsent(name, ignored -> new ArrayList<>()).add(field);
            }
        }
        unsupported.forEach(indexes::remove);
        return new Evidence(new ArrayList<>(indexes.values()), !unsupported.isEmpty());
    }

    static Evidence mongo(PdkConnector connector, String table) throws Throwable {
        List<List<String>> keys = new ArrayList<>();
        boolean opaque = false;
        List<Map<?, ?>> rows = execute(connector, "aggregate", Map.of("collection", table,
                "pipeline", List.of(Map.of("$indexStats", Map.of())), "batchSize", 1000));
        if (rows.isEmpty()) throw new IllegalStateException("target index inspection returned no index specs");
        for (Map<?, ?> row : rows) {
            if (!(row.get("spec") instanceof Map<?, ?> spec) || !(spec.get("key") instanceof Map<?, ?> fields)) {
                throw new IllegalStateException("target index inspection returned an incomplete index spec");
            }
            if (!Boolean.TRUE.equals(spec.get("unique")) && !"_id_".equals(spec.get("name"))) continue;
            if (Boolean.TRUE.equals(spec.get("sparse")) || spec.containsKey("partialFilterExpression")
                    || spec.containsKey("collation") || fields.isEmpty()
                    || fields.keySet().stream().anyMatch(key -> !(key instanceof String))
                    || fields.values().stream().anyMatch(value -> !(value instanceof Number number)
                            || (number.intValue() != 1 && number.intValue() != -1))) {
                opaque = true;
                continue;
            }
            keys.add(fields.keySet().stream().map(String.class::cast).toList());
        }
        return new Evidence(keys, opaque);
    }

    private static List<Map<?, ?>> query(PdkConnector connector, String sql) throws Throwable {
        return execute(connector, "executeQuery", Map.of("sql", sql, "batchSize", 1000));
    }

    private static List<Map<?, ?>> execute(PdkConnector connector, String command, Map<String, Object> params) throws Throwable {
        var execute = connector.functions().getExecuteCommandFunction();
        if (execute == null) throw new IllegalStateException("exact target index inspection is unavailable");
        List<Map<?, ?>> rows = new ArrayList<>();
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
                            rows.add(values);
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
