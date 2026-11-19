package io.tapstate.e2e;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Versioned operator recipes expressed through the same pipeline DSL as ordinary runs. */
final class BenchmarkOperators {
    static final String PARENT = "parent_identity";
    static final String ORDINAL = "element_ordinal";
    private static final Set<String> PAIRED = Set.of("join-hot-update", "nest-child-update", "nest-fanout");

    final String scenario;
    final Map<String, Integer> rows;
    private final List<String> nodes;
    private final BsonDocument specification;

    final Integer fanout;

    BenchmarkOperators(BsonDocument definition, int requestedRows, boolean variant) {
        this.scenario = definition.getString("spec").getValue()
                .replaceFirst("^specs/", "").replaceFirst("\\.json$", "");
        var spec = definition.getDocument("workload_spec").getDocument("scenario");
        specification = spec;
        nodes = spec.getArray("graph").stream().map(BsonValue::asDocument)
                .map(node -> node.getString("id").getValue()).toList();
        fanout = scenario.equals("nest-fanout") ? spec.getDocument("parameters").getDocument("paired_control")
                .getDocument(variant ? "variant_trial" : "baseline").getInt32("children_per_root").getValue() : null;
        rows = switch (scenario) {
            case "map-basic", "filter-basic", "js-basic", "mixed-double-map" -> Map.of("records", requestedRows);
            case "unwind-basic" -> Map.of("parents", requestedRows);
            case "union-basic" -> Map.of("left", positive(requestedRows / 2), "right", requestedRows - requestedRows / 2);
            case "join-basic", "join-steady-state", "join-lifecycle-release" -> Map.of("facts", requestedRows, "dimensions", Math.max(1, requestedRows / 10));
            case "join-hot-update" -> Map.of("facts", Math.multiplyExact(requestedRows, 10), "dimensions", requestedRows);
            case "nest-fanout" -> {
                if (requestedRows < 4 || requestedRows % 4 != 0) {
                    throw new IllegalArgumentException("The baseline child count must be a positive multiple of four");
                }
                yield Map.of("roots", requestedRows / 4, "children", Math.multiplyExact(requestedRows / 4, fanout));
            }
            case "nest-basic", "nest-child-update", "mixed-map-nest" -> {
                int fanout = spec.getDocument("inputs").getDocument("roots").getInt32("children_per_root").getValue();
                if (requestedRows < fanout || requestedRows % fanout != 0) {
                    throw new IllegalArgumentException("Child input count must be a positive multiple of " + fanout);
                }
                yield Map.of("roots", requestedRows / fanout, "children", requestedRows);
            }
            default -> throw new IllegalArgumentException("No operator execution path for " + scenario);
        };
    }

    private static int positive(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("Both Union inputs need rows");
        }
        return count;
    }

    boolean paired() {
        return PAIRED.contains(scenario);
    }

    List<String> sources() {
        return rows.keySet().stream().sorted().toList();
    }

    List<String> targets() {
        return switch (scenario) {
            case "unwind-basic" -> List.of("parents");
            case "union-basic" -> List.of("left", "right");
            case "join-basic", "join-hot-update", "join-steady-state", "join-lifecycle-release" -> List.of("facts");
            case "nest-basic", "nest-child-update", "nest-fanout" -> List.of("roots");
            case "mixed-map-nest" -> List.of(nodes.getFirst());
            default -> List.of("records");
        };
    }

    Map<String, Object> pipeline(String id, boolean snapshot, MongoDatabase source) {
        String first = nodes.getFirst();
        List<Map<String, Object>> graph = switch (scenario) {
            case "map-basic" -> List.of(map(first, List.of("records"), projection(source)));
            case "filter-basic" -> List.of(filter(first, List.of("records")));
            case "js-basic" -> List.of(script(first, List.of("records"), true));
            case "unwind-basic" -> List.of(Map.of("id", first, "from", List.of("parents"), "type", "unwind",
                    "path", "items", "element_key", "k", "element_type", "json",
                    "source_id_field", PARENT, "include_array_index", ORDINAL));
            case "union-basic" -> List.of(Map.of("id", first, "from", List.of("left", "right"), "type", "union"));
            case "join-basic", "join-hot-update", "join-steady-state", "join-lifecycle-release" -> List.of(Map.of(
                    "id", first, "type", "join", "engine", "builtin", "from", Map.of("f", "facts", "d", "dimensions"),
                    "sql", """
                            SELECT f._id AS _id, f.dimension_id AS dimension_id,
                                   f.payload AS fact_value, d.score AS dimension_value
                            FROM f JOIN d ON f.dimension_id = d._id
                            """));
            case "nest-basic", "nest-child-update", "nest-fanout" -> List.of(nest(first, "roots", "_id"));
            case "mixed-double-map" -> List.of(
                    map(first, List.of("records"), Map.of("value", "=int(after.value) + 1")),
                    filter(nodes.get(1), first),
                    map(nodes.get(2), nodes.get(1), Map.of("value", "=int(after.value) * 2")),
                    script(nodes.get(3), nodes.get(2), false));
            case "mixed-map-nest" -> List.of(map(first, List.of("roots"), Map.of("root_key", "$_id")),
                    nest(nodes.get(1), first, "root_key"));
            default -> throw new AssertionError(scenario);
        };
        var sink = Map.of("id", "output", "source", "tgt_perf", "write_mode", "upsert", "on_full_load", "clear");
        return Map.of("version", "tapstate/v1", "kind", "pipeline", "id", id, "source", "src_perf",
                "settings", Map.of("read_mode", snapshot ? "snapshot_only" : "snapshot_and_cdc"),
                "transforms", graph, "serve", Map.of("from", nodes.getLast(), "sync", List.of(sink)));
    }

    private static Map<String, Object> projection(MongoDatabase source) {
        var row = source.getCollection("records", BsonDocument.class).find().first();
        if (row == null) {
            throw new IllegalStateException("Map projection requires a discovered source shape");
        }
        var result = new LinkedHashMap<String, Object>();
        for (String field : row.keySet()) {
            if (!Set.of("_id", "payload", "label").contains(field)) {
                result.put(field, false);
            }
        }
        result.put("display_label", "$label");
        result.put("marker", "mapped");
        result.put("adjusted_value", "=int(after.value) + 1");
        return result;
    }

    private static Map<String, Object> map(String id, Object from, Map<String, Object> fields) {
        return Map.of("id", id, "from", from, "type", "map", "fields", fields);
    }

    private static Map<String, Object> filter(String id, Object from) {
        return Map.of("id", id, "from", from, "type", "filter",
                "expr", "op == 'd' ? int(before._id) % 2 == 0 : int(after._id) % 2 == 0");
    }

    private static Map<String, Object> script(String id, Object from, boolean changeValue) {
        String operation = changeValue ? "row.value = Number(row.value) + 7;" : "";
        return Map.of("id", id, "from", from, "type", "js", "script", """
                function process(record) {
                  for (const row of [record.before, record.after]) {
                    if (row != null) {
                      %s
                      row.label = "js-" + row.label;
                    }
                  }
                  return record;
                }
                """.formatted(operation));
    }

    private static Map<String, Object> nest(String id, String roots, String rootKey) {
        var embed = Map.of("from", "c", "on", Map.of("root_id", rootKey), "as", "array",
                "path", "children", "arrayKey", List.of("_id"));
        return Map.of("id", id, "type", "nest", "from", Map.of("r", roots, "c", "children"),
                "root", Map.of("from", "r", "key", List.of(rootKey), "embed", List.of(embed)));
    }

    Map<String, BsonDocument> expected(MongoDatabase source) {
        var byTable = new LinkedHashMap<String, List<BsonDocument>>();
        for (String table : sources()) {
            byTable.put(table, source.getCollection(table, BsonDocument.class).find()
                    .sort(Sorts.ascending("_id")).into(new ArrayList<>()));
        }
        return BenchmarkOperatorOracle.expected(scenario, byTable);
    }

    List<String> mutableInputs() {
        var inputs = specification.getDocument("inputs");
        return sources().stream().filter(input -> inputs.getDocument(input).getBoolean("receives_changes").getValue())
                .toList();
    }
}
