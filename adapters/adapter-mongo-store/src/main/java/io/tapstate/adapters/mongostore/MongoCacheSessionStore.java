package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** OP session semantics on the cache handle's fixed authentication generation. */
final class MongoCacheSessionStore implements SessionStore {
    private static final FindOneAndUpdateOptions AFTER = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    private static final FindOneAndUpdateOptions BEFORE = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE);
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "secretHash", "principal", "scope", "issuer", "revoked",
            "createdAt", "lastUsedAt", "idleExpiresAt", "absoluteExpiresAt");
    private final MongoCacheIdentity facts;
    private final Runnable requireStructures;
    private final MongoCollection<Document> sessions;

    MongoCacheSessionStore(MongoCacheIdentity facts, MongoDatabase database, Runnable requireStructures) {
        this.facts = Objects.requireNonNull(facts, "facts");
        this.requireStructures = Objects.requireNonNull(requireStructures, "requireStructures");
        if (facts.scope().mode() != StorageBinding.Mode.OP) throw new IllegalArgumentException("OP sessions require an OP cache scope");
        this.sessions = SystemCollections.OP_SESSIONS.on(database);
    }

    @Override public void save(SessionRecord record) {
        Objects.requireNonNull(record, "record");
        Document captured = MongoSessionStore.toDocument(record).append("formatVersion", 1);
        captured.put("_id", key(record.sessionId()));
        transaction(session -> {
            facts.fence(session);
            sessions.replaceOne(session, new Document("_id", key(record.sessionId())), captured, new ReplaceOptions().upsert(true));
            return null;
        });
    }

    @Override public Optional<SessionRecord> find(String id) {
        Objects.requireNonNull(id, "id");
        return transaction(session -> {
            facts.requireReady(session);
            return Optional.ofNullable(sessions.find(session, new Document("_id", key(id))).first()).map(row -> decode(id, row));
        });
    }

    @Override public Optional<SessionRecord> exchange(String id, String hash, String issuer, Instant now, Instant idleExpiresAt) {
        arguments(id, hash, issuer, now); Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
        return transaction(session -> {
            facts.fence(session);
            Document filter = live(id, hash, issuer, now).append("revoked", false);
            var update = List.of(new Document("$set", new Document("lastUsedAt", now.toEpochMilli())
                    .append("idleExpiresAt", new Document("$min", List.of("$absoluteExpiresAt", idleExpiresAt.toEpochMilli())))));
            return Optional.ofNullable(sessions.findOneAndUpdate(session, filter, update, AFTER)).map(row -> decode(id, row));
        });
    }

    @Override public boolean revoke(String id, String hash, String issuer, Instant now) {
        arguments(id, hash, issuer, now);
        return transaction(session -> {
            facts.fence(session);
            Document row = sessions.findOneAndUpdate(session, live(id, hash, issuer, now),
                    new Document("$set", new Document("revoked", true)), BEFORE);
            if (row == null) return false;
            decode(id, row);
            return true;
        });
    }

    /** Revocation only: no lifetime extension or authorization is obtained from an expired browser credential. */
    @Override public boolean revokeBrowserSession(String id, String hash, String issuer) {
        Objects.requireNonNull(id, "id"); Objects.requireNonNull(hash, "hash"); Objects.requireNonNull(issuer, "issuer");
        return transaction(session -> {
            facts.fence(session);
            Document filter = new Document("_id", key(id)).append("secretHash", hash).append("issuer", issuer);
            Document row = sessions.findOneAndUpdate(session, filter,
                    new Document("$set", new Document("revoked", true)), BEFORE);
            if (row == null) return false;
            decode(id, row);
            return true;
        });
    }

    private Document live(String id, String hash, String issuer, Instant now) {
        return new Document("_id", key(id)).append("secretHash", hash).append("issuer", issuer)
                .append("idleExpiresAt", new Document("$gt", now.toEpochMilli()))
                .append("absoluteExpiresAt", new Document("$gt", now.toEpochMilli()));
    }
    private SessionRecord decode(String id, Document row) {
        if (!row.keySet().equals(FIELDS) || !Integer.valueOf(1).equals(row.get("formatVersion")) || !key(id).equals(row.get("_id"))) throw unreadable();
        Document payload = new Document(row); payload.remove("formatVersion"); payload.put("_id", id);
        try { return MongoSessionStore.toRecord(payload); }
        catch (TapstateException invalid) {
            if (invalid.code() != IoError.DOCUMENT_UNREADABLE) throw invalid;
            throw unreadable();
        }
    }
    private String key(String id) { return JsonWriter.write(List.of("cache-auth", facts.scope().cacheIncarnation(), facts.scope().authGeneration(), id)); }
    private static void arguments(String id, String hash, String issuer, Instant now) {
        Objects.requireNonNull(id, "id"); Objects.requireNonNull(hash, "hash"); Objects.requireNonNull(issuer, "issuer"); Objects.requireNonNull(now, "now");
    }
    private <T> T transaction(Function<ClientSession, T> operation) {
        return facts.transaction(session -> {
            // Check structure before the session's first command; retries retain the original scope.
            requireStructures.run();
            return operation.apply(session);
        });
    }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "op-session", "field", "session"), null); }
}
