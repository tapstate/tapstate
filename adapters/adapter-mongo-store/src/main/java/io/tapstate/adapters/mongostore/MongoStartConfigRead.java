package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Bounded input slices for synchronous Start, using one CONFIG snapshot per read. */
final class MongoStartConfigRead {
    private static final int MAX_ROWS_PER_LOOKUP = 16;
    private static final int MAX_INLINE_BYTES = 64 * 1024;

    private MongoStartConfigRead() {}

    static List<Document> root() {
        return new ArrayList<>(List.of(new Document("$match", new Document("_id", "cluster"))));
    }

    static void addBinding(List<Document> stages) {
        stages.add(MongoValidationClaimObservation.lookup(SystemCollections.STORAGE_BINDING, "binding", "cluster", null));
        stages.add(MongoValidationClaimObservation.lookup(SystemCollections.STORAGE_SETUP, "setup", "cluster", null));
        stages.add(MongoValidationClaimObservation.lookup(SystemCollections.STORAGE_RECOVERY, "recovery", "cluster", null));
    }

    static void addArtifacts(List<Document> stages, List<String> ids) {
        addRows(stages, SystemCollections.SOURCES, "sources", ids);
        addRows(stages, SystemCollections.PIPELINES, "pipelines", ids);
        addRows(stages, SystemCollections.TRANSFORM_DEFINITIONS, "transforms", ids);
        addRows(stages, SystemCollections.VIEW_DEFINITIONS, "views", ids);
        addRows(stages, SystemCollections.SERVE_DEFINITIONS, "serves", ids);
    }

    private static void addRows(List<Document> stages, SystemCollections collection, String field, List<String> ids) {
        var bytes = new Document("$bsonSize", "$$ROOT");
        var projection = new Document("_id", 1).append("value", new Document("$cond",
                List.of(new Document("$lte", List.of(bytes, MAX_INLINE_BYTES)), "$$ROOT", false)));
        var filter = new Document("_id", new Document("$in", ids));
        stages.add(new Document("$lookup", new Document("from", collection.collectionName())
                .append("as", field).append("pipeline", List.of(new Document("$match", filter),
                        new Document("$limit", MAX_ROWS_PER_LOOKUP + 1), new Document("$project", projection)))));
    }

    static Optional<List<Document>> artifacts(Document row, List<String> ids) {
        if (row == null) return Optional.empty();
        var result = new ArrayList<Document>();
        var seen = new java.util.HashSet<String>();
        for (var slot : List.of(java.util.Map.entry("sources", "source"), java.util.Map.entry("pipelines", "pipeline"),
                java.util.Map.entry("transforms", "transform"), java.util.Map.entry("views", "view"), java.util.Map.entry("serves", "serve"))) {
            var rows = row.getList(slot.getKey(), Document.class);
            if (rows.size() > MAX_ROWS_PER_LOOKUP) return Optional.empty();
            for (Document item : rows) {
                if (!(item.get("_id") instanceof String id) || !ids.contains(id)
                        || !seen.add(id) || !(item.get("value") instanceof Document value)
                        || !slot.getValue().equals(value.get("kind"))) return Optional.empty();
                result.add(value);
            }
        }
        return Optional.of(List.copyOf(result));
    }

    static Document snapshot(MongoDatabase database, List<Document> stages) {
        return SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database.withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.SNAPSHOT)).aggregate(stages).first();
    }

    static Document snapshot(MongoDatabase database, ClientSession session, List<Document> stages) {
        return SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).aggregate(session, stages).first();
    }

    static Optional<Document> row(Document snapshot, String field, Object id) {
        if (snapshot == null || !(snapshot.get(field) instanceof List<?> rows) || rows.size() != 1
                || !(rows.getFirst() instanceof Document value) || !exactId(id, value.get("_id"))) return Optional.empty();
        return Optional.of(value);
    }

    private static boolean exactId(Object expected, Object actual) {
        if (expected instanceof Document document)
            return actual instanceof Document found && new ArrayList<>(document.keySet()).equals(new ArrayList<>(found.keySet()))
                    && document.equals(found);
        return expected.equals(actual);
    }

    static Optional<StorageBinding> binding(Document snapshot, MongoStorageBindingStore bindings, CacheBusinessScope scope) {
        if (snapshot == null || !"cluster".equals(snapshot.get("_id"))
                || !scope.clusterId().equals(snapshot.get("clusterId"))
                || !Long.valueOf(scope.bindingVersion()).equals(snapshot.get("bindingVersion"))
                || !MongoConfigurationDomain.LAYOUT.equals(snapshot.get("layoutId"))) return Optional.empty();
        var bound = row(snapshot, "binding", "cluster");
        var setup = row(snapshot, "setup", "cluster");
        var recovery = row(snapshot, "recovery", "cluster");
        if (bound.isEmpty() || setup.isEmpty() || recovery.isEmpty()) return Optional.empty();
        return Optional.of(bindings.observationBinding(scope, bound.orElseThrow(), setup.orElseThrow(), recovery.orElseThrow()));
    }
}
