package io.tapstate.adapters.pdk;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal PDK metadata contract; native index semantics are verified separately against databases. */
public final class IdentityConnectorFixture {
    private IdentityConnectorFixture() { }

    static PdkConnector guarded(String id, ConnectorRef ref, Map<String, Object> settings, Runnable active) {
        var node = new io.tapstate.core.model.PipelineNode("flow", "sink");
        var purpose = io.tapstate.core.model.PipelineNode.Purpose.SYNC;
        var local = new io.tapstate.spi.store.RequiredNoteScope(
                io.tapstate.spi.store.RequiredNoteOwner.forNode(1, node, purpose, 1), 1);
        var global = new io.tapstate.spi.store.RequiredNoteScope(new io.tapstate.spi.store.RequiredNoteOwner(
                1, ConnectorStateNamespace.GLOBAL, io.tapstate.spi.store.RequiredNoteOwner.Kind.GLOBAL, "cluster", 0), 1);
        var store = (io.tapstate.spi.store.RequiredNoteStore) java.lang.reflect.Proxy.newProxyInstance(
                io.tapstate.spi.store.RequiredNoteStore.class.getClassLoader(),
                new Class<?>[]{io.tapstate.spi.store.RequiredNoteStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "guardedBy" -> proxy;
                    case "origin" -> ConnectorNoteOrigin.capture(id, ref.contentHash(), settings);
                    default -> throw new AssertionError("unexpected notes access: " + method.getName());
                });
        var scopes = ConnectorNoteScopes.of(store,
                namespace -> namespace.equals(local.owner().namespace()) ? local : global, active);
        return PdkConnector.open(id, ref, settings, node, null, null, scopes, purpose);
    }

    public static ConnectorRef ref(Path directory) {
        Path jar = SyntheticJar.compileToJar(directory, "synthetic.IdentityConnector", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.*;
                import io.tapdata.entity.schema.*;
                import java.util.*;
                import java.util.function.Consumer;
                public class IdentityConnector implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry codecs) {
                        f.supportGetTableNamesFunction((c, size, out) -> out.accept(
                            Boolean.FALSE.equals(c.getConnectionConfig().get("exists")) ? List.of() : List.of("orders")));
                        f.supportQueryIndexes((c, table, out) -> {
                            List<TapIndex> indexes = new ArrayList<>();
                            Object rows = c.getConnectionConfig().getOrDefault("rows", List.of());
                            if (rows instanceof List<?> values) for (Object value : values) {
                                if (!(value instanceof Map<?, ?> row) || !(row.get("spec") instanceof Map<?, ?> spec)
                                    || !(spec.get("key") instanceof Map<?, ?> key)) continue;
                                TapIndex index = new TapIndex().name((String) spec.get("name"))
                                    .unique(Boolean.TRUE.equals(spec.get("unique")) || "_id_".equals(spec.get("name")));
                                key.forEach((name, direction) -> index.indexField(new TapIndexField().name((String) name)
                                    .fieldAsc(!Integer.valueOf(-1).equals(direction))));
                                indexes.add(index);
                            }
                            out.accept(indexes);
                        });
                        f.supportCountByPartitionFilterFunction((c, table, filter) ->
                            ((Number) c.getConnectionConfig().getOrDefault("count", 0L)).longValue());
                        f.supportCreateIndex((c, table, event) -> {});
                        f.supportWriteRecord((c, events, table, out) ->
                            out.accept(new WriteListResult<io.tapdata.entity.event.dml.TapRecordEvent>().insertedCount(events.size())));
                        f.supportExecuteCommandFunction((c, command, out) -> {
                            Object pipeline = command.getParams().get("pipeline");
                            boolean pointCheck = pipeline instanceof List<?> stages && !stages.isEmpty()
                                && stages.get(0) instanceof Map<?, ?> first && first.containsKey("$match");
                            out.accept(new ExecuteResult<>().result(pointCheck ? List.of(Map.of("rows", List.of()))
                                : c.getConnectionConfig().getOrDefault("rows", List.of())));
                        });
                    }
                    public void init(TapConnectionContext c) {
                        if (Boolean.TRUE.equals(c.getConnectionConfig().get("initFails"))) throw new IllegalStateException("init failed");
                    }
                    public void stop(TapConnectionContext c) { }
                    public void discoverSchema(TapConnectionContext c, List<String> names, int limit, Consumer<List<TapTable>> out) {
                        if (Boolean.TRUE.equals(c.getConnectionConfig().get("discoverFails"))) throw new IllegalStateException("discovery failed");
                        if (Boolean.FALSE.equals(c.getConnectionConfig().get("exists"))) { out.accept(List.of()); return; }
                        TapTable table = new TapTable("orders");
                        for (String name : List.of("id", "_unwind_source_id", "position")) {
                            if (name.equals(c.getConnectionConfig().get("omitColumn"))) continue;
                            table.add(new TapField(name, "bigint").isPrimaryKey(name.equals("id"))
                                .primaryKeyPos(name.equals("id") ? 1 : null)
                                .autoInc(name.equals("id") && !Boolean.FALSE.equals(c.getConnectionConfig().get("generated")))
                                .nullable(Boolean.TRUE.equals(c.getConnectionConfig().get("nullable"))));
                        }
                        out.accept(List.of(table));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> out) { return ConnectionOptions.create(); }
                    public int tableCount(TapConnectionContext c) { return 1; }
                }
                """);
        return new ConnectorRef(List.of(jar), "synthetic.IdentityConnector", "2.0.8", null, null, "a".repeat(64), false);
    }

    static Map<String, Object> postgresRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("is_valid", true);
        row.put("is_ready", true);
        row.put("deterministic", true);
        row.put("columns_json", "[\"_unwind_source_id\",\"position\"]");
        row.put("int64_columns_json", "[\"_unwind_source_id\",\"position\"]");
        return row;
    }

    static Map<String, Object> mysqlRow(String field) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("index_name", "logical_key");
        row.put("column_name", field);
        row.put("column_type", "bigint(20)");
        row.put("generated_expression", "");
        return row;
    }

    static Map<String, Object> mongoIndex(String name, Map<?, ?> fields) {
        return new LinkedHashMap<>(Map.of("spec", new LinkedHashMap<>(Map.of("name", name, "key", fields, "unique", true))));
    }

    static List<Map<String, Object>> mongoIndexes() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("_unwind_source_id", 1);
        fields.put("position", -1);
        return List.of(mongoIndex("_id_", Map.of("_id", 1)), mongoIndex("logical_key", fields));
    }
}
