package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/** Read-only catalog comparison over the managed namespaces selected by the storage owner. */
public final class MongoStructureInspection {
    private static final int REPAIR_BYTES = 512;
    private static final SystemCollections.IndexSpec ID = new SystemCollections.IndexSpec(List.of("_id"), true, "_id_");
    private static final SystemCollections.IndexSpec FILES = new SystemCollections.IndexSpec(
            List.of("filename", "uploadDate"), false, "filename_1_uploadDate_1");
    private static final SystemCollections.IndexSpec CHUNKS = new SystemCollections.IndexSpec(
            List.of("files_id", "n"), true, "files_id_1_n_1");

    public enum Problem { COLLECTION_MISSING, COLLECTION_TYPE, COLLECTION_OPTIONS,
        INDEX_MISSING, INDEX_DEFINITION, UNEXPECTED_CONSTRAINT, UNEXPECTED_INDEX }

    public record Difference(SystemCollections.Database domain, String database, String collection, String index,
            Problem problem, String expected, String actual, String repairCommand) {
        public String description() {
            return domain + " " + database + "." + collection + (index == null ? "" : " [" + index + "]")
                    + " " + problem + " (expected " + expected + ", observed " + actual + ")";
        }

        public boolean requiresSupport() { return repairCommand == null; }
    }

    record Namespace(SystemCollections descriptor, String physicalName, List<SystemCollections.IndexSpec> indexes,
            boolean required) {}

    private MongoStructureInspection() {}

    static List<Namespace> namespaces(Collection<SystemCollections> selected, Collection<SystemCollections> required,
            Duration retention) {
        var result = new ArrayList<Namespace>();
        for (var table : selected) {
            var indexes = new ArrayList<>(table.indexes());
            indexes.addFirst(ID);
            if (table == SystemCollections.CACHE_PIPELINE_RATE_HISTORY)
                indexes.set(1, new SystemCollections.IndexSpec(List.of("observedAt"), false, retention.toSeconds()));
            if (table.kind() == SystemCollections.Kind.GRIDFS_BUCKET) indexes.add(FILES);
            result.add(new Namespace(table, table.indexTarget(), List.copyOf(indexes), required.contains(table)));
            if (table.kind() == SystemCollections.Kind.GRIDFS_BUCKET)
                result.add(new Namespace(table, table.physicalNames().get(1), List.of(ID, CHUNKS), required.contains(table)));
        }
        return List.copyOf(result);
    }

    static List<Difference> inspect(MongoDatabase database, List<Namespace> selected, MongoReadBudget budget) {
        return StoreIo.call(() -> {
            if (selected.isEmpty()) return List.of();
            var names = selected.stream().map(Namespace::physicalName).toList();
            var collections = new HashMap<String, Document>();
            for (var row : MongoCollectionMetadata.matching(budget.database(database), names, true))
                collections.put(row.getString("name"), row);
            var differences = new ArrayList<Difference>();
            for (var namespace : selected) {
                budget.remainingMillis();
                var row = collections.get(namespace.physicalName());
                if (row == null) {
                    if (namespace.required())
                        differences.add(difference(database.getName(), namespace, null, Problem.COLLECTION_MISSING,
                                "collection", "absent", null));
                    continue;
                }
                if (!"collection".equals(row.get("type"))) {
                    differences.add(difference(database.getName(), namespace, null, Problem.COLLECTION_TYPE,
                            "collection", "view".equals(row.get("type")) ? "view" : "other namespace type", null));
                    continue;
                }
                var optionDifference = collectionOptionsDifference(database.getName(), namespace, row);
                if (optionDifference != null) differences.add(optionDifference);
                var bounded = budget.database(database);
                var handle = namespace.physicalName().equals(namespace.descriptor().indexTarget())
                        ? namespace.descriptor().indexTargetOn(bounded) : namespace.descriptor().chunksOn(bounded);
                var indexes = handle.listIndexes().timeoutMode(TimeoutMode.CURSOR_LIFETIME).into(new ArrayList<>());
                var indexDifferences = compareIndexes(database.getName(), namespace, indexes);
                boolean manualRepair = optionDifference == null && indexDifferences.size() == 1;
                for (var item : indexDifferences)
                    differences.add(manualRepair ? item : difference(item.database(), namespace, item.index(),
                            item.problem(), item.expected(), item.actual(), null));
            }
            budget.remainingMillis();
            return List.copyOf(differences);
        });
    }

    static Difference collectionOptionsDifference(String database, Namespace namespace, Document row) {
        var options = row.get("options", Document.class);
        if (MongoAlertStorageSchema.COLLECTIONS.contains(namespace.descriptor()) && (options == null || !options.isEmpty())) {
            return difference(database, namespace, null, Problem.COLLECTION_OPTIONS,
                    "empty collection options", options == null ? "unreadable options" : "nonempty collection options", null);
        }
        if (options == null) return null;
        String observed = null;
        if (Boolean.TRUE.equals(options.get("capped"))) observed = "capped";
        else if (options.containsKey("timeseries")) observed = "time series";
        else if (namespace.descriptor() == SystemCollections.CAPTURE_PREPARATIONS && options.containsKey("collation"))
            observed = "custom collation";
        return observed == null ? null : difference(database, namespace, null, Problem.COLLECTION_OPTIONS,
                "supported collection options", observed, null);
    }

    static List<Difference> compareIndexes(String database, Namespace namespace, List<Document> indexes) {
        var actual = new LinkedHashMap<String, Document>();
        indexes.forEach(index -> actual.put(index.getString("name"), index));
        var differences = new ArrayList<Difference>();
        for (var expected : namespace.indexes()) {
            var index = actual.remove(expected.indexName());
            var specification = expectedOptions(expected);
            boolean ordinaryAlertIndex = MongoAlertStorageSchema.COLLECTIONS.contains(namespace.descriptor());
            if (ordinaryAlertIndex) specification.append("customCollation", false);
            var definition = new Document("key", expected.keyDocument()).append("options", specification).toJson();
            if (index == null) {
                boolean equivalent = indexes.stream().anyMatch(row -> sameKey(expected.keyDocument(), row.get("key")));
                var command = equivalent ? null : repair(database, namespace, expected);
                differences.add(difference(database, namespace, expected.indexName(), Problem.INDEX_MISSING,
                        definition, "absent", command));
                continue;
            }
            var observed = observedOptions(index, expected);
            if (ordinaryAlertIndex) observed.append("customCollation", index.containsKey("collation"));
            if (!sameKey(expected.keyDocument(), index.get("key")) || !specification.equals(observed)) {
                differences.add(difference(database, namespace, expected.indexName(), Problem.INDEX_DEFINITION,
                        definition, "key " + observedKey(index.get("key")) + " options " + observed.toJson(), null));
            }
        }
        for (var entry : actual.entrySet()) {
            var index = entry.getValue();
            if (Boolean.TRUE.equals(index.get("unique")) || index.containsKey("prepareUnique")
                    || index.containsKey("expireAfterSeconds")) {
                differences.add(difference(database, namespace, safeName(entry.getKey()), Problem.UNEXPECTED_CONSTRAINT,
                        "no undeclared unique or expiry constraint", index.containsKey("expireAfterSeconds") ? "TTL index" : "unique index", null));
            } else if (MongoAlertStorageSchema.COLLECTIONS.contains(namespace.descriptor())
                    || namespace.descriptor() == SystemCollections.CONFIG_AI_PROVIDER_SETTINGS) {
                differences.add(difference(database, namespace, safeName(entry.getKey()), Problem.UNEXPECTED_INDEX,
                        "declared indexes only", "additional index", null));
            }
        }
        return List.copyOf(differences);
    }

    private static Document expectedOptions(SystemCollections.IndexSpec spec) {
        return new Document("unique", spec.unique()).append("expireAfterSeconds", spec.expireAfterSeconds())
                .append("hidden", false).append("sparse", false).append("prepareUnique", false)
                .append("partial", spec.partialFilterJson() == null ? null : Document.parse(spec.partialFilterJson()));
    }

    private static Document observedOptions(Document observed, SystemCollections.IndexSpec spec) {
        var ttl = observed.get("expireAfterSeconds");
        var expectedPartial = spec.partialFilterJson() == null ? null : Document.parse(spec.partialFilterJson());
        var partial = observed.get("partialFilterExpression");
        // The foreign predicate may contain sensitive data, so expose only whether it matches.
        return new Document("unique", spec == ID || Boolean.TRUE.equals(observed.get("unique")))
                .append("expireAfterSeconds", ttl instanceof Integer || ttl instanceof Long ? ((Number) ttl).longValue() : ttl == null ? null : "invalid")
                .append("hidden", Boolean.TRUE.equals(observed.get("hidden")))
                .append("sparse", Boolean.TRUE.equals(observed.get("sparse")))
                .append("prepareUnique", observed.containsKey("prepareUnique"))
                .append("partial", Objects.equals(expectedPartial, partial) ? expectedPartial : "different");
    }

    private static boolean sameKey(Document expected, Object observed) {
        return observed instanceof Document key && expected.equals(key)
                && List.copyOf(expected.keySet()).equals(List.copyOf(key.keySet()));
    }

    private static String observedKey(Object observed) {
        if (!(observed instanceof Document keys) || keys.size() > 16) return "different";
        var value = new Document();
        keys.forEach((key, direction) -> value.put(safeName(key), direction instanceof Number ? direction : "other"));
        return value.toJson();
    }

    private static String repair(String database, Namespace namespace, SystemCollections.IndexSpec spec) {
        if (spec.unique() || spec.expireAfterSeconds() != null || spec.partialFilterJson() != null) return null;
        String command = "db.getSiblingDB(" + string(database) + ").getCollection(" + string(namespace.physicalName())
                + ").createIndex(" + spec.keyDocument().toJson() + ", " + new Document("name", spec.indexName()).toJson() + ")";
        return command.getBytes(StandardCharsets.UTF_8).length <= REPAIR_BYTES ? command : null;
    }

    private static String string(String value) { return io.tapstate.core.common.JsonWriter.write(value); }

    private static String safeName(String value) {
        return value != null && value.matches("[a-zA-Z0-9_.-]{1,128}") ? value : "<unrecognized name>";
    }

    private static Difference difference(String database, Namespace namespace, String index, Problem problem,
            String expected, String actual, String repairCommand) {
        return new Difference(namespace.descriptor().database(), database, namespace.physicalName(), index, problem,
                expected, actual, repairCommand);
    }
}
