package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
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

    @Override public void record(String pipelineId, String stepId, Map<String, String> schema, String statement, String derivedFrom, String derivedBy) {
        String key = key(pipelineId, stepId);
        var captured = new DerivedSchema(0, schema, statement, derivedFrom, derivedBy);
        try {
            Document encoded = MongoDerivedSchemaStore.toDocument(captured);
            if (!encoded.equals(MongoDerivedSchemaStore.toDocument(MongoDerivedSchemaStore.toDerivedSchema(encoded, pipelineId)))) throw unreadable();
        } catch (TapstateException | IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw new TapstateException(IoError.DOCUMENT_INVALID, Map.of("id", key, "field", "schema"), null);
        }
        access.metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, true, session -> {
            Stored before = read(session, key); requireIntactHistory(before);
            var versions = new ArrayList<DerivedSchema>(before == null ? List.of() : before.versions());
            DerivedSchema last = versions.isEmpty() ? null : versions.getLast();
            if (last != null && last.schema().equals(captured.schema())) {
                versions.set(versions.size() - 1, new DerivedSchema(last.version(), last.schema(), statement, derivedFrom, derivedBy));
            } else {
                versions.add(new DerivedSchema(versions.size(), captured.schema(), statement, derivedFrom, derivedBy));
            }
            write(session, key, versions, before == null ? null : before.pin()); return null;
        });
    }

    @Override public void pin(String pipelineId, String stepId, long version) {
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

    private Stored read(ClientSession session, String key) {
        Document row = rows.find(session, new Document("_id", key)).first();
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
        var encoded = new ArrayList<Document>(); for (var version : versions) encoded.add(MongoDerivedSchemaStore.toDocument(version));
        var row = new Document("_id", key).append("scope", MongoPipelinePreparationFacts.scope(access.scope)).append("versions", encoded);
        if (pin != null) row.append("pin", pin);
        rows.replaceOne(session, new Document("_id", key), row, new ReplaceOptions().upsert(true));
    }
    private String key(String pipelineId, String stepId) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId")); Objects.requireNonNull(stepId, "stepId");
        if (pipelineId.indexOf('.') >= 0) throw new IllegalArgumentException("pipelineId must not contain '.'");
        return pipelineId + '.' + stepId;
    }
    private TapstateException unreadable() { return access.unreadable("derived-schema"); }
    private record Stored(List<DerivedSchema> versions, Long pin) {}
}
