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
    private final boolean extendedWrites;

    MongoCacheConnectionTestResultStore(MongoCacheMetadataAccess access) { this(access, false); }

    MongoCacheConnectionTestResultStore(MongoCacheMetadataAccess access, boolean extendedWrites) {
        this.access = access;
        this.extendedWrites = extendedWrites;
    }

    @Override public Publication beginOrdinaryTest(String connectionId) {
        if (!extendedWrites) return ConnectionTestResultStore.super.beginOrdinaryTest(connectionId);
        Document key = new Document("_id", connectionId);
        Document baseline = access.read(session -> access.collection.find(session, key).first());
        requireOrdinaryScope(baseline, connectionId);
        return result -> {
            if (!connectionId.equals(result.connectionId())) throw new IllegalArgumentException("test result identity changed");
            var report = MongoConnectionDiagnostic.fit(result, ignored -> true);
            var captured = access.stamp(MongoConnectionTestResultStore.compatibleCacheDocument(report));
            return access.write(session -> {
                var current = access.collection.find(session, key).first();
                requireOrdinaryScope(current, connectionId);
                // A completed test still returns its own result; a changed row belongs to another publication.
                if (Objects.equals(baseline, current)) {
                    access.collection.replaceOne(session, key, captured, new ReplaceOptions().upsert(true));
                }
                return report;
            });
        };
    }

    private void requireOrdinaryScope(Document row, String id) {
        if (row == null) return;
        if (!access.inScope(row, id)) throw MongoCacheMetadataAccess.unreadable(id, "cache-scope");
        if (row.containsKey("formatVersion")) throw MongoCacheMetadataAccess.unreadable(id, "attempt");
        try {
            MongoConnectionTestResultStore.strictCacheResult(access.payload(row));
        } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
            throw MongoCacheMetadataAccess.unreadable(id, "result");
        }
    }
    @Override public void save(ConnectionTestResult result) {
        if (extendedWrites) throw new IllegalStateException("extended connection results require a captured baseline");
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
                return Optional.of(MongoConnectionTestResultStore.strictCacheResult(access.payload(row)));
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
        Document captured = bounded(value, false);
        return finish(value, controlledTimeout, captured);
    }

    @Override public Optional<ConnectionTestResult> publishDefaultReport(DefaultSourceTestAttempt value, boolean controlledTimeout) {
        if (!extendedWrites) return ConnectionTestResultStore.super.publishDefaultReport(value, controlledTimeout);
        if (value.report() == null || value.state() == DefaultSourceTestAttempt.State.CHECKING
                || (controlledTimeout && value.state() != DefaultSourceTestAttempt.State.FAILED)) {
            throw new IllegalArgumentException("invalid report terminal");
        }
        ConnectionTestResult report = MongoConnectionDiagnostic.fit(value.report(), candidate ->
                MongoConnectionDiagnostic.bsonSize(access.stamp(document(withReport(value, candidate), true))) <= 1024 * 1024);
        var terminal = withReport(value, report);
        return finish(terminal, controlledTimeout, bounded(terminal, true)) ? Optional.of(report) : Optional.empty();
    }

    private static DefaultSourceTestAttempt withReport(DefaultSourceTestAttempt source, ConnectionTestResult report) {
        return new DefaultSourceTestAttempt(source.connectionId(), source.connectorId(), source.sourceHash(), source.attemptId(),
                source.owner(), source.sessionGeneration(), source.acceptedAt(), source.deadlineAt(), source.state(), source.finishedAt(), report);
    }

    private boolean finish(DefaultSourceTestAttempt value, boolean controlledTimeout, Document captured) {
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
        return bounded(value, false);
    }
    private Document bounded(DefaultSourceTestAttempt value, boolean includeDiagnostics) {
        Document captured = access.stamp(document(value, includeDiagnostics));
        requireBounded(captured, value.connectionId());
        return captured;
    }
    static void requireBounded(Document row, String id) {
        if (MongoConnectionDiagnostic.bsonSize(row) > 1024 * 1024)
            throw new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), null);
    }
    static Document document(DefaultSourceTestAttempt value) {
        return document(value, false);
    }
    private static Document document(DefaultSourceTestAttempt value, boolean includeDiagnostics) {
        var report = value.report();
        return new Document("_id", value.connectionId()).append("formatVersion", 2).append("connectorId", value.connectorId())
                .append("sourceHash", value.sourceHash()).append("attemptId", value.attemptId())
                .append("nodeId", value.owner().nodeId()).append("bootId", value.owner().bootId())
                .append("sessionGeneration", value.sessionGeneration()).append("acceptedAt", value.acceptedAt())
                .append("deadlineAt", value.deadlineAt()).append("state", value.state().name()).append("finishedAt", value.finishedAt())
                .append("report", report == null ? null : includeDiagnostics
                        ? MongoConnectionTestResultStore.compatibleCacheDocument(report)
                        : MongoConnectionTestResultStore.toDocument(report));
    }
    static DefaultSourceTestAttempt attempt(Document row) {
        String id = String.valueOf(row.get("_id"));
        try {
            if (!Integer.valueOf(2).equals(row.get("formatVersion"))) throw new IllegalArgumentException("unsupported attempt format");
            var value = new DefaultSourceTestAttempt(row.getString("_id"), row.getString("connectorId"), row.getString("sourceHash"),
                    row.getString("attemptId"), new WorkloadOwner(row.getString("nodeId"), row.getString("bootId")),
                    row.getLong("sessionGeneration"), row.getLong("acceptedAt"), row.getLong("deadlineAt"),
                    DefaultSourceTestAttempt.State.valueOf(row.getString("state")), row.getLong("finishedAt"),
                    row.get("report") == null ? null : MongoConnectionTestResultStore.strictCacheResult(row.get("report", Document.class)));
            if (!row.equals(document(value, true))) throw new IllegalArgumentException("noncanonical attempt");
            return value;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw MongoCacheMetadataAccess.unreadable(id, "attempt");
        }
    }

}
