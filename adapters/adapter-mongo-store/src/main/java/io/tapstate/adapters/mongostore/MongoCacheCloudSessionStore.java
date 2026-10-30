package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;

import java.time.Instant;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Managed sessions and revocation-first markers stay separate but compete in the same cache transaction. */
final class MongoCacheCloudSessionStore implements CloudSessionStore {
    private static final FindOneAndUpdateOptions AFTER = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    private static final Set<String> LOGIN = Set.of("_id", "formatVersion", "origin", "issuer", "organizationId", "clusterId", "jwtId",
            "secretHash", "userId", "scope", "revoked", "createdAt", "lastUsedAt", "idleExpiresAt");
    private static final Set<String> DENIAL = Set.of("_id", "formatVersion", "issuer", "organizationId", "clusterId", "jwtId", "revokedAt");
    private final MongoCacheIdentity facts;
    private final Runnable requireStructures;
    private final MongoCollection<Document> sessions;
    private final MongoCollection<Document> revocations;
    private final MongoReadBudget budget;

    MongoCacheCloudSessionStore(MongoCacheIdentity facts, MongoDatabase database, Runnable requireStructures) {
        this(facts, database, requireStructures, null);
    }

    MongoCacheCloudSessionStore(MongoCacheIdentity facts, MongoDatabase database, Runnable requireStructures, MongoReadBudget budget) {
        this.budget = budget;
        this.facts = Objects.requireNonNull(facts, "facts");
        this.requireStructures = Objects.requireNonNull(requireStructures, "requireStructures");
        if (facts.scope().mode() != StorageBinding.Mode.CLOUD) throw new IllegalArgumentException("Cloud sessions require a managed cache scope");
        this.sessions = SystemCollections.CLOUD_SESSIONS.on(database);
        this.revocations = SystemCollections.CLOUD_REVOCATIONS.on(database);
    }

    @Override public boolean create(CloudSessionRecord record) {
        Objects.requireNonNull(record, "record");
        Document captured = MongoCloudSessionStore.toDocument(record).append("formatVersion", 1);
        captured.put("_id", key(record.identity(), record.jwtId()));
        return transaction(session -> {
            facts.fence(session);
            if (!local(record.identity()) || denied(session, record.identity(), record.jwtId())) return false;
            if (sessions.find(session, new Document("_id", key(record.identity(), record.jwtId()))).first() != null) return false;
            sessions.insertOne(session, captured);
            return true;
        });
    }

    @Override public Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jwtId) {
        String key = key(identity, jwtId);
        return transaction(session -> {
            facts.requireReady(session);
            if (!local(identity)) return Optional.empty();
            return Optional.ofNullable(sessions.find(session, new Document("_id", key)).first()).map(row -> decode(identity, jwtId, row));
        });
    }

    @Override public Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity, String jwtId, String hash, Instant now, Instant idleExpiresAt) {
        requireMutation(hash, now); Objects.requireNonNull(idleExpiresAt, "idleExpiresAt"); key(identity, jwtId);
        return transaction(session -> {
            facts.fence(session);
            if (!local(identity) || denied(session, identity, jwtId)) return Optional.empty();
            Document filter = context(identity, jwtId).append("secretHash", hash).append("revoked", false)
                    .append("idleExpiresAt", new Document("$gt", now.toEpochMilli()));
            Document update = new Document("$max", new Document("lastUsedAt", now.toEpochMilli()).append("idleExpiresAt", idleExpiresAt.toEpochMilli()));
            return Optional.ofNullable(sessions.findOneAndUpdate(session, filter, update, AFTER)).map(row -> decode(identity, jwtId, row));
        });
    }

    @Override public boolean logout(CloudSessionIdentity identity, String jwtId, String hash, Instant now) {
        requireMutation(hash, now); key(identity, jwtId);
        return transaction(session -> {
            facts.fence(session);
            if (!local(identity)) return false;
            Document matched = sessions.findOneAndUpdate(session, context(identity, jwtId).append("secretHash", hash),
                    new Document("$set", new Document("revoked", true)), AFTER);
            if (matched == null) return false;
            decode(identity, jwtId, matched);
            deny(session, identity, jwtId, now);
            return true;
        });
    }

    @Override public void invalidate(CloudSessionIdentity identity, String jwtId, Instant now) {
        Objects.requireNonNull(now, "now"); key(identity, jwtId);
        transaction(session -> {
            facts.fence(session);
            if (!local(identity)) throw MongoCacheIdentity.fenced();
            deny(session, identity, jwtId, now);
            sessions.updateOne(session, context(identity, jwtId), new Document("$set", new Document("revoked", true)));
            return null;
        });
    }

    private boolean denied(ClientSession session, CloudSessionIdentity identity, String jwtId) {
        Document row = revocations.find(session, new Document("_id", key(identity, jwtId))).first();
        if (row == null) return false;
        if (!row.keySet().equals(DENIAL) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !identity.issuer().equals(row.get("issuer")) || !identity.organizationId().equals(row.get("organizationId"))
                || !identity.clusterId().equals(row.get("clusterId")) || !jwtId.equals(row.get("jwtId")) || !(row.get("revokedAt") instanceof Long)) throw unreadable();
        return true;
    }
    private void deny(ClientSession session, CloudSessionIdentity identity, String jwtId, Instant now) {
        if (denied(session, identity, jwtId)) return;
        revocations.insertOne(session, new Document("_id", key(identity, jwtId)).append("formatVersion", 1)
                .append("issuer", identity.issuer()).append("organizationId", identity.organizationId()).append("clusterId", identity.clusterId())
                .append("jwtId", jwtId).append("revokedAt", now.toEpochMilli()));
    }
    private CloudSessionRecord decode(CloudSessionIdentity identity, String jwtId, Document row) {
        Set<String> fields = new HashSet<>(LOGIN); if (row.containsKey("clusterContext")) fields.add("clusterContext");
        if (!row.keySet().equals(fields) || !Integer.valueOf(1).equals(row.get("formatVersion")) || !key(identity, jwtId).equals(row.get("_id"))) throw unreadable();
        Document payload = new Document(row); payload.remove("formatVersion"); payload.put("_id", MongoCloudSessionStore.sessionKey(identity, jwtId));
        return MongoCloudSessionStore.toRecord(payload).orElseThrow(MongoCacheCloudSessionStore::unreadable);
    }
    private Document context(CloudSessionIdentity identity, String jwtId) {
        return new Document("_id", key(identity, jwtId)).append("origin", "cloud").append("issuer", identity.issuer())
                .append("organizationId", identity.organizationId()).append("clusterId", identity.clusterId()).append("jwtId", jwtId);
    }
    private String key(CloudSessionIdentity identity, String jwtId) {
        String logical = Base64.getUrlEncoder().withoutPadding().encodeToString(MongoCloudSessionStore.sessionKey(identity, jwtId).getData());
        return JsonWriter.write(List.of("cache-auth", facts.scope().cacheIncarnation(), facts.scope().authGeneration(), logical));
    }
    private boolean local(CloudSessionIdentity identity) { return facts.scope().clusterId().equals(identity.clusterId()); }
    private static void requireMutation(String hash, Instant now) {
        if (hash == null || hash.isBlank()) throw new IllegalArgumentException("session secret hash must not be blank");
        Objects.requireNonNull(now, "now");
    }
    private <T> T transaction(Function<ClientSession, T> operation) {
        Function<ClientSession, T> verified = session -> {
            // Check structure before the session's first command; retries retain the original scope.
            requireStructures.run();
            return operation.apply(session);
        };
        return budget == null ? facts.transaction(verified) : facts.transaction(budget, verified);
    }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cloud-session", "field", "session"), null); }
}
