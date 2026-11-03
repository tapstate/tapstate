package io.tapstate.adapters.mongostore;

import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.DefaultSourceTestAttempt;
import io.tapstate.spi.store.WorkloadOwner;
import java.util.Map;
import org.bson.Document;

import java.util.Objects;
import java.util.Optional;

/** Latest diagnostics in one original CACHE scope; diagnostic presence never grants setup authority. */
final class MongoCacheConnectionTestResultStore implements ConnectionTestResultStore {
    private final MongoCacheMetadataAccess access;
    MongoCacheConnectionTestResultStore(MongoCacheMetadataAccess access) { this.access = access; }
    @Override public void save(ConnectionTestResult result) {
        Document captured = access.stamp(MongoConnectionTestResultStore.toDocument(Objects.requireNonNull(result, "result")));
        access.write(session -> {
            Document previous = access.collection.find(session, new Document("_id", result.connectionId())).first();
            if (previous != null && previous.containsKey("formatVersion")) throw MongoCacheMetadataAccess.unreadable(result.connectionId(), "attempt");
            access.collection.replaceOne(session, new Document("_id", result.connectionId()), captured, new ReplaceOptions().upsert(true)); return null;
        });
    }
    @Override public Optional<ConnectionTestResult> find(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        return access.read(session -> {
            Document row = access.collection.find(session, new Document("_id", connectionId)).first();
            if (row == null || !access.inScope(row, connectionId)) return Optional.empty();
            if (row.containsKey("formatVersion")) {
                requireBounded(row, connectionId);
                var attempt = attempt(access.payload(row));
                return Optional.ofNullable(attempt.report());
            }
            try {
                Document payload = access.payload(row); var result = MongoConnectionTestResultStore.toResult(payload);
                if (!payload.equals(MongoConnectionTestResultStore.toDocument(result))) throw MongoCacheMetadataAccess.unreadable(connectionId, "result");
                return Optional.of(result);
            } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
                throw MongoCacheMetadataAccess.unreadable(connectionId, "result");
            }
        });
    }
    @Override public void beginDefaultAttempt(DefaultSourceTestAttempt value) {
        if (value.state() != DefaultSourceTestAttempt.State.CHECKING) throw new IllegalArgumentException("new attempt must be checking");
        Document captured = bounded(value);
        access.write(session -> {
            Document previous = access.collection.find(session, new Document("_id", value.connectionId())).first();
            if (previous != null && !access.inScope(previous, value.connectionId())) throw MongoCacheMetadataAccess.unreadable(value.connectionId(), "cache-scope");
            access.collection.replaceOne(session, new Document("_id", value.connectionId()), captured, new ReplaceOptions().upsert(true));
            return null;
        });
    }
    @Override public boolean finishDefaultAttempt(DefaultSourceTestAttempt value, boolean controlledTimeout) {
        if (value.state() == DefaultSourceTestAttempt.State.CHECKING) throw new IllegalArgumentException("terminal attempt required");
        if (controlledTimeout && value.state() != DefaultSourceTestAttempt.State.FAILED) throw new IllegalArgumentException("invalid timeout terminal");
        Document captured = bounded(value);
        return access.write(session -> {
            Document filter = new Document("_id", value.connectionId()).append("formatVersion", 2)
                    .append("sourceHash", value.sourceHash()).append("attemptId", value.attemptId()).append("state", "CHECKING")
                    .append("connectorId", value.connectorId()).append("nodeId", value.owner().nodeId()).append("bootId", value.owner().bootId())
                    .append("sessionGeneration", value.sessionGeneration()).append("acceptedAt", value.acceptedAt()).append("deadlineAt", value.deadlineAt())
                    .append("cacheScope", captured.get("cacheScope"));
            if (value.state() != DefaultSourceTestAttempt.State.UNKNOWN && !controlledTimeout)
                filter.append("$expr", new Document("$gte", java.util.List.of("$deadlineAt", new Document("$toLong", "$$NOW"))));
            return access.collection.replaceOne(session, filter, captured).getModifiedCount() == 1;
        });
    }
    @Override public Optional<DefaultSourceTestAttempt> findDefaultAttempt(String connectionId) {
        return access.read(session -> {
            var row = access.collection.find(session, new Document("_id", connectionId)).first();
            if (row == null || !access.inScope(row, connectionId) || !row.containsKey("formatVersion")) return Optional.empty();
            requireBounded(row, connectionId);
            return Optional.of(attempt(access.payload(row)));
        });
    }
    private Document bounded(DefaultSourceTestAttempt value) {
        Document captured = access.stamp(document(value));
        requireBounded(captured, value.connectionId());
        return captured;
    }
    static void requireBounded(Document row, String id) {
        try (var output = new org.bson.io.BasicOutputBuffer(); var writer = new org.bson.BsonBinaryWriter(output)) {
            new org.bson.codecs.DocumentCodec().encode(writer, row, org.bson.codecs.EncoderContext.builder().build());
            if (output.getSize() > 1024 * 1024)
                throw new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), null);
        }
    }
    static Document document(DefaultSourceTestAttempt value) {
        return new Document("_id", value.connectionId()).append("formatVersion", 2).append("connectorId", value.connectorId())
                .append("sourceHash", value.sourceHash()).append("attemptId", value.attemptId())
                .append("nodeId", value.owner().nodeId()).append("bootId", value.owner().bootId())
                .append("sessionGeneration", value.sessionGeneration()).append("acceptedAt", value.acceptedAt())
                .append("deadlineAt", value.deadlineAt()).append("state", value.state().name()).append("finishedAt", value.finishedAt())
                .append("report", value.report() == null ? null : MongoConnectionTestResultStore.toDocument(value.report()));
    }
    static DefaultSourceTestAttempt attempt(Document row) {
        String id = String.valueOf(row.get("_id"));
        try {
            if (!Integer.valueOf(2).equals(row.get("formatVersion"))) throw new IllegalArgumentException("unsupported attempt format");
            var value = new DefaultSourceTestAttempt(row.getString("_id"), row.getString("connectorId"), row.getString("sourceHash"),
                    row.getString("attemptId"), new WorkloadOwner(row.getString("nodeId"), row.getString("bootId")),
                    row.getLong("sessionGeneration"), row.getLong("acceptedAt"), row.getLong("deadlineAt"),
                    DefaultSourceTestAttempt.State.valueOf(row.getString("state")), row.getLong("finishedAt"),
                    row.get("report") == null ? null : MongoConnectionTestResultStore.toResult(row.get("report", Document.class)));
            if (!row.equals(document(value))) throw new IllegalArgumentException("noncanonical attempt");
            return value;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw MongoCacheMetadataAccess.unreadable(id, "attempt");
        }
    }

}
