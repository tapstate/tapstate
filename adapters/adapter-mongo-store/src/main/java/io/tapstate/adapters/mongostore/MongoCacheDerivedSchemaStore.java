package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReplaceOneModel;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Per-step history and pin retained by one original Pipeline state generation and execution claim. */
final class MongoCacheDerivedSchemaStore implements DerivedSchemaStore {
    private final MongoCachePipelineAccess access;
    private final MongoCollection<Document> rows;

    MongoCacheDerivedSchemaStore(MongoCachePipelineAccess access) {
        this.access = Objects.requireNonNull(access, "access");
        this.rows = SystemCollections.CACHE_DERIVED_SCHEMAS.on(access.database);
    }

    @Override public Optional<DerivedSchema> latest(String pipelineId, String stepId) {
        String key = key(pipelineId, stepId);
        return access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, false, session -> {
            Stored row = read(session, key);
            return row == null ? Optional.empty() : Optional.of(row.versions().getLast());
        });
    }

    @Override public Map<String, DerivedSchema> latestMany(String pipelineId, Set<String> stepIds) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId"));
        var requested = new java.util.LinkedHashMap<String, String>();
        for (String step : Set.copyOf(stepIds)) requested.put(key(pipelineId, step), step);
        return access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, false, session -> {
            var found = new java.util.LinkedHashMap<String, DerivedSchema>();
            try (var cursor = rows.find(session, new Document("_id", new Document("$in", requested.keySet()))).batchSize(64).iterator()) {
                while (cursor.hasNext()) {
                    var raw = cursor.next();
                    if (!(raw.get("_id") instanceof String id) || !requested.containsKey(id)) throw unreadable();
                    Stored row = decode(id, raw);
                    if (row != null && found.put(requested.get(id), row.versions().getLast()) != null) throw unreadable();
                }
            }
            return java.util.Collections.unmodifiableMap(found);
        });
    }

    @Override public void record(String pipelineId, String stepId, Map<String, String> schema, String statement, String derivedFrom, String derivedBy) {
        String key = key(pipelineId, stepId);
        var captured = new DerivedSchema(0, schema, statement, derivedFrom, derivedBy);
        validateChange(pipelineId, key, captured);
        access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, true, session -> {
            Stored updated = applyChange(read(session, key), captured);
            write(session, key, updated.versions(), updated.pin()); return null;
        });
    }

    @Override public void recordBatch(String pipelineId, List<Recording> recordings) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId"));
        var captured = List.copyOf(recordings).stream().map(recording -> {
            String key = key(pipelineId, recording.stepId());
            var schema = new DerivedSchema(0, recording.schema(), recording.statement(),
                    recording.derivedFrom(), recording.derivedBy());
            validateChange(pipelineId, key, schema);
            return Map.entry(key, schema);
        }).toList();
        if (captured.isEmpty()) return;
        access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, true, session -> {
            for (var recording : captured) {
                String key = recording.getKey();
                var updated = applyChange(read(session, key), recording.getValue());
                write(session, key, updated.versions(), updated.pin());
            }
            return null;
        });
    }

    @Override public void recordMany(String pipelineId, List<Change> changes) {
        recordMany(pipelineId, changes, false);
    }

    @Override public void recordAndPinMany(String pipelineId, List<Change> changes) {
        access.requireModelPin();
        recordMany(pipelineId, changes, true);
    }

    private void recordMany(String pipelineId, List<Change> changes, boolean pinLatest) {
        var batch = new ArrayList<Change>();
        long bytes = 0;
        for (Change change : List.copyOf(changes)) {
            String key = key(pipelineId, change.stepId());
            DerivedSchema value = change.derivation();
            validateChange(pipelineId, key, value);
            long size = bytes(MongoDerivedSchemaStore.toDocument(value)) + 4L * key.length();
            if (!batch.isEmpty() && (batch.size() >= 64 || bytes + size > 4 * 1024 * 1024)) {
                writeBatch(pipelineId, batch, pinLatest);
                batch.clear();
                bytes = 0;
            }
            batch.add(change);
            bytes += size;
        }
        if (!batch.isEmpty()) writeBatch(pipelineId, batch, pinLatest);
    }

    private void writeBatch(String pipelineId, List<Change> batch, boolean pinLatest) {
        access.publicationModelsTransaction(session -> {
            // A non-simple collection can resolve a key to an alias. Keep its original point semantics.
            var collection = access.database.listCollections()
                    .filter(new Document("name", SystemCollections.CACHE_DERIVED_SCHEMAS.collectionName())).first();
            var options = collection == null ? null : collection.get("options", Document.class);
            if (options != null && options.containsKey("collation")) {
                for (var change : batch) {
                    String key = key(pipelineId, change.stepId());
                    var updated = applyChange(read(session, key), change.derivation());
                    write(session, key, updated.versions(), selectedPin(updated, pinLatest));
                }
                return null;
            }
            var pending = new LinkedHashMap<String, List<DerivedSchema>>();
            for (var change : batch) pending.computeIfAbsent(key(pipelineId, change.stepId()), ignored -> new ArrayList<>())
                    .add(change.derivation());
            var writes = new ArrayList<ReplaceOneModel<Document>>();
            long writeBytes = 0;
            try (var cursor = rows.find(session, new Document("_id", new Document("$in", List.copyOf(pending.keySet()))))
                    .batchSize(32).iterator()) {
                while (cursor.hasNext()) {
                    var row = cursor.next();
                    String key = row.getString("_id");
                    var changes = pending.remove(key);
                    if (changes == null) throw unreadable();
                    var updated = applyChanges(decode(key, row), changes);
                    var replacement = document(key, updated.versions(), selectedPin(updated, pinLatest));
                    writeBytes = addWrite(session, writes, writeBytes, key, replacement);
                }
            }
            for (var entry : pending.entrySet()) {
                var updated = applyChanges(null, entry.getValue());
                writeBytes = addWrite(session, writes, writeBytes, entry.getKey(),
                        document(entry.getKey(), updated.versions(), selectedPin(updated, pinLatest)));
            }
            if (!writes.isEmpty()) rows.bulkWrite(session, writes);
            return null;
        });
    }

    private static Long selectedPin(Stored updated, boolean pinLatest) {
        if (pinLatest) return updated.versions().getLast().version();
        return updated.pin();
    }

    private long addWrite(ClientSession session, List<ReplaceOneModel<Document>> writes, long used,
            String key, Document replacement) {
        long size = bytes(replacement);
        if (!writes.isEmpty() && used + size > 4 * 1024 * 1024) {
            rows.bulkWrite(session, writes);
            writes.clear();
            used = 0;
        }
        writes.add(new ReplaceOneModel<>(new Document("_id", key), replacement, new ReplaceOptions().upsert(true)));
        return used + size;
    }

    private static int bytes(Document value) {
        return new org.bson.RawBsonDocument(value, new org.bson.codecs.DocumentCodec()).getByteBuffer().remaining();
    }

    private void validateChange(String pipelineId, String key, DerivedSchema captured) {
        try {
            Document encoded = MongoDerivedSchemaStore.toDocument(captured);
            if (!encoded.equals(MongoDerivedSchemaStore.toDocument(MongoDerivedSchemaStore.toDerivedSchema(encoded, pipelineId)))) throw unreadable();
        } catch (TapstateException | IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw new TapstateException(IoError.DOCUMENT_INVALID, Map.of("id", key, "field", "schema"), null);
        }
    }

    private Stored applyChanges(Stored before, List<DerivedSchema> changes) {
        Stored updated = before;
        for (var change : changes) updated = applyChange(updated, change);
        return updated;
    }

    private Stored applyChange(Stored before, DerivedSchema captured) {
        requireIntactHistory(before);
        var versions = new ArrayList<DerivedSchema>(before == null ? List.of() : before.versions());
        DerivedSchema last = versions.isEmpty() ? null : versions.getLast();
        if (last != null && last.schema().equals(captured.schema())) {
            versions.set(versions.size() - 1, new DerivedSchema(last.version(), last.schema(), captured.statement(),
                    captured.derivedFrom(), captured.derivedBy()));
        } else {
            versions.add(new DerivedSchema(versions.size(), captured.schema(), captured.statement(),
                    captured.derivedFrom(), captured.derivedBy()));
        }
        return new Stored(versions, before == null ? null : before.pin());
    }

    @Override public void pin(String pipelineId, String stepId, long version) {
        access.requireModelPin();
        String key = key(pipelineId, stepId);
        if (version < 0) throw new IllegalArgumentException("a derived schema pin is non-negative");
        access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, true, session -> {
            Stored row = read(session, key); requireIntactHistory(row);
            if (row == null || version >= row.versions().size()) throw new TapstateException(IoError.DOCUMENT_INVALID, Map.of("id", key, "field", "pin"), null);
            write(session, key, row.versions(), version); return null;
        });
    }

    @Override public Optional<DerivedSchema> pinned(String pipelineId, String stepId) {
        String key = key(pipelineId, stepId);
        return access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, false, session -> {
            Stored row = read(session, key);
            return row == null || row.pin() == null || row.pin() >= row.versions().size()
                    ? Optional.empty() : Optional.of(row.versions().get(row.pin().intValue()));
        });
    }

    @Override public void delete(String pipelineId) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId"));
        access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, true, session -> {
            var filter = new Document("_id", new Document("$gte", pipelineId + '.').append("$lt", pipelineId + '/'))
                    .append("scope", MongoPipelinePreparationFacts.scope(access.scope));
            rows.deleteMany(session, filter); return null;
        });
    }

    /** Called only inside the original retirement's CACHE transaction after its exact authorization is fenced. */
    void retireInTransaction(ClientSession session, boolean unprepared) {
        for (Document row : rows.find(session, pipelineRows())) {
            if (!(row.get("_id") instanceof String id) || !MongoPipelinePreparationFacts.scope(access.scope).equals(row.get("scope"))) throw unreadable();
            Stored stored = read(session, id);
            if (stored == null || unprepared && stored.pin() != null) throw unreadable();
            requireIntactHistory(stored);
        }
        rows.deleteMany(session, pipelineRows().append("scope", MongoPipelinePreparationFacts.scope(access.scope)));
    }
    void requireAbsentInTransaction(ClientSession session) {
        if (rows.find(session, pipelineRows()).limit(1).first() != null) throw unreadable();
    }
    private Document pipelineRows() {
        String pipeline = access.scope.pipelineId();
        return new Document("$or", List.of(new Document("_id", new Document("$gte", pipeline + '.').append("$lt", pipeline + '/')),
                new Document("scope.pipelineId", pipeline)));
    }

    private Stored read(ClientSession session, String key) {
        Document row = rows.find(session, new Document("_id", key)).first();
        return decode(key, row);
    }

    private Stored decode(String key, Document row) {
        if (row == null) return null;
        try {
            if (!(row.get("scope") instanceof Document scope)) throw unreadable();
            var decoded = MongoPipelinePreparationFacts.scope(scope);
            if (!access.scope.pipelineId().equals(decoded.pipelineId())) throw unreadable();
            if (!access.scope.equals(decoded)) return null;
            if (!key.equals(row.get("_id")) || !(row.keySet().equals(Set.of("_id", "scope", "versions"))
                    || row.keySet().equals(Set.of("_id", "scope", "versions", "pin"))) || !(row.get("versions") instanceof List<?> raw) || raw.isEmpty()) throw unreadable();
            var versions = new ArrayList<DerivedSchema>();
            for (Object item : raw) {
                if (!(item instanceof Document entry) || !Long.valueOf(versions.size()).equals(entry.get("version"))) throw unreadable();
                DerivedSchema version = MongoDerivedSchemaStore.toDerivedSchema(entry, access.scope.pipelineId());
                if (!entry.equals(MongoDerivedSchemaStore.toDocument(version))) throw unreadable();
                versions.add(version);
            }
            Long pin = null;
            if (row.containsKey("pin")) {
                access.requireModelPin();
                if (!(row.get("pin") instanceof Long value) || value < 0) throw unreadable();
                pin = value;
            }
            return new Stored(List.copyOf(versions), pin);
        } catch (TapstateException | IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private void requireIntactHistory(Stored row) {
        // A missing held version must not be reassigned to a different model in this generation.
        if (row != null && row.pin() != null && row.pin() >= row.versions().size()) throw unreadable();
    }

    private void write(ClientSession session, String key, List<DerivedSchema> versions, Long pin) {
        rows.replaceOne(session, new Document("_id", key), document(key, versions, pin), new ReplaceOptions().upsert(true));
    }

    private Document document(String key, List<DerivedSchema> versions, Long pin) {
        var encoded = new ArrayList<Document>(); for (var version : versions) encoded.add(MongoDerivedSchemaStore.toDocument(version));
        var row = new Document("_id", key).append("scope", MongoPipelinePreparationFacts.scope(access.scope)).append("versions", encoded);
        if (pin != null) row.append("pin", pin);
        return row;
    }
    private String key(String pipelineId, String stepId) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId")); Objects.requireNonNull(stepId, "stepId");
        if (pipelineId.indexOf('.') >= 0) throw new IllegalArgumentException("pipelineId must not contain '.'");
        return pipelineId + '.' + stepId;
    }
    private TapstateException unreadable() { return access.unreadable("derived-schema"); }
    private record Stored(List<DerivedSchema> versions, Long pin) {}
}
