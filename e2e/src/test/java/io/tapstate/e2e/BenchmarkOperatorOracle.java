package io.tapstate.e2e;

import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

/** Computes expected target identities and contents from source BSON, apart from the DSL and engine. */
final class BenchmarkOperatorOracle {
    private BenchmarkOperatorOracle() {
    }

    static Map<String, BsonDocument> expected(String scenario, Map<String, List<BsonDocument>> source) {
        var result = new LinkedHashMap<String, BsonDocument>();
        switch (scenario) {
            case "map-basic" -> source.get("records").forEach(row ->
                    put(result, key(row, "_id"), new BsonDocument("_id", row.get("_id"))
                            .append("payload", row.get("payload")).append("display_label", row.get("label"))
                            .append("marker", new BsonString("mapped")).append("adjusted_value",
                                    new BsonInt64(number(row, "value") + 1))));
            case "filter-basic" -> source.get("records").stream().filter(BenchmarkOperatorOracle::even)
                    .forEach(row -> put(result, key(row, "_id"), row));
            case "js-basic" -> source.get("records").forEach(row ->
                    put(result, key(row, "_id"), script(row, number(row, "value") + 7)));
            case "mixed-double-map" -> source.get("records").stream().filter(BenchmarkOperatorOracle::even)
                    .forEach(row -> put(result, key(row, "_id"), script(row, (number(row, "value") + 1) * 2)));
            case "unwind-basic" -> source.get("parents").forEach(parent -> {
                if (!parent.containsKey("items") || parent.get("items").isNull()) {
                    return;
                }
                var items = parent.getArray("items");
                for (int index = 0; index < items.size(); index++) {
                    var element = items.get(index).asDocument();
                    var expanded = parent.clone();
                    expanded.remove("_id");
                    expanded.put("items", element);
                    expanded.put("k", element.get("k"));
                    expanded.put(BenchmarkOperators.PARENT, parent.get("_id"));
                    expanded.put(BenchmarkOperators.ORDINAL, new BsonInt64(index));
                    put(result, unwindKey(expanded), expanded);
                }
            });
            case "union-basic" -> source.forEach((table, rows) ->
                    rows.forEach(row -> put(result, table + "/" + key(row, "_id"), row)));
            case "join-basic", "join-hot-update" -> {
                var byKey = new HashMap<Long, BsonDocument>();
                source.get("dimensions").forEach(row -> byKey.put(number(row, "_id"), row));
                for (var fact : source.get("facts")) {
                    var dimension = byKey.get(number(fact, "dimension_id"));
                    if (dimension != null) {
                        put(result, key(fact, "_id"), new BsonDocument("_id", fact.get("_id"))
                                .append("dimension_id", fact.get("dimension_id"))
                                .append("fact_value", fact.get("payload")).append("dimension_value", dimension.get("score")));
                    }
                }
            }
            case "nest-basic", "nest-child-update", "mixed-map-nest" -> {
                var children = new HashMap<Long, List<BsonValue>>();
                for (var row : source.get("children")) {
                    children.computeIfAbsent(number(row, "root_id"), ignored -> new ArrayList<>()).add(row);
                }
                for (var root : source.get("roots")) {
                    var document = root.clone();
                    if (scenario.equals("mixed-map-nest")) {
                        document.put("root_key", document.remove("_id"));
                    }
                    document.put("children", new BsonArray(children.getOrDefault(number(root, "_id"), List.of())));
                    put(result, key(root, "_id"), document);
                }
            }
            default -> throw new AssertionError(scenario);
        }
        return result;
    }

    static String difference(BenchmarkOperators operators, MongoDatabase database, Map<String, BsonDocument> expected) {
        var seen = new java.util.HashSet<String>();
        for (String table : operators.targets()) {
            try (var cursor = database.getCollection(table, BsonDocument.class).find().cursor()) {
                while (cursor.hasNext()) {
                    var actual = cursor.next();
                    String identity = targetIdentity(operators.scenario, table, actual);
                    if (!seen.add(identity)) {
                        return "Duplicated target identity " + identity;
                    }
                    BsonDocument reference = expected.get(identity);
                    if (reference == null || !equivalent(reference, actual)) {
                        return "Target identity " + identity + " differs"
                                + System.lineSeparator() + "Expected " + reference
                                + System.lineSeparator() + "Actual " + actual;
                    }
                }
            }
        }
        return seen.size() == expected.size() ? null
                : "Target row count " + seen.size() + " differs from " + expected.size() + " in " + operators.targets();
    }

    private static String targetIdentity(String scenario, String table, BsonDocument actual) {
        return switch (scenario) {
            case "unwind-basic" -> {
                actual.remove("_id");
                yield unwindKey(actual);
            }
            case "mixed-map-nest" -> {
                actual.remove("_id");
                yield key(actual, "root_key");
            }
            case "union-basic" -> table + "/" + key(actual, "_id");
            default -> key(actual, "_id");
        };
    }

    private static String unwindKey(BsonDocument expanded) {
        return key(expanded, BenchmarkOperators.PARENT) + "/" + key(expanded, "k");
    }

    private static long number(BsonDocument row, String field) {
        return row.getNumber(field).longValue();
    }

    private static String key(BsonDocument row, String field) {
        BsonValue key = row.get(field);
        return key != null && key.isNumber() ? Long.toString(key.asNumber().longValue()) : String.valueOf(key);
    }

    private static boolean even(BsonDocument row) {
        return number(row, "_id") % 2 == 0;
    }

    private static BsonDocument script(BsonDocument source, long value) {
        var row = source.clone();
        row.put("value", new BsonInt64(value));
        row.put("label", new BsonString("js-" + source.getString("label").getValue()));
        return row;
    }

    private static void put(Map<String, BsonDocument> result, String key, BsonDocument value) {
        if (result.putIfAbsent(key, value) != null) {
            throw new IllegalStateException("Duplicate expected row identity " + key);
        }
    }

    static boolean equivalent(BsonValue expected, BsonValue actual) {
        if (expected == null || actual == null) {
            return expected == actual;
        }
        if (expected.isNumber() && actual.isNumber()) {
            return expected.asNumber().decimal128Value().bigDecimalValue()
                    .compareTo(actual.asNumber().decimal128Value().bigDecimalValue()) == 0;
        }
        if (expected.isDocument() && actual.isDocument()) {
            var left = expected.asDocument();
            var right = actual.asDocument();
            return left.keySet().equals(right.keySet())
                    && left.entrySet().stream().allMatch(entry -> equivalent(entry.getValue(), right.get(entry.getKey())));
        }
        if (expected.isArray() && actual.isArray()) {
            var left = expected.asArray();
            var right = actual.asArray();
            if (left.size() != right.size()) {
                return false;
            }
            for (int index = 0; index < left.size(); index++) {
                if (!equivalent(left.get(index), right.get(index))) {
                    return false;
                }
            }
            return true;
        }
        return expected.equals(actual);
    }
}
