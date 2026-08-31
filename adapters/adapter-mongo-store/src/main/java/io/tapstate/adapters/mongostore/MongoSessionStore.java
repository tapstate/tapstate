package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** MongoDB persistence for opaque user sessions, including their atomic exchange and revocation guards. */
public final class MongoSessionStore implements SessionStore {

    private static final FindOneAndUpdateOptions RETURN_AFTER =
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);

    private final MongoCollection<Document> collection;

    public MongoSessionStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(SessionRecord record) {
        Objects.requireNonNull(record, "record");
        StoreIo.run(() -> collection.replaceOne(new Document("_id", record.sessionId()), toDocument(record),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<SessionRecord> find(String sessionId) {
        Objects.requireNonNull(sessionId, "sessionId");
        Document found = StoreIo.call(() -> collection.find(new Document("_id", sessionId)).first());
        return found == null ? Optional.empty() : Optional.of(toRecord(found));
    }

    @Override
    public Optional<SessionRecord> exchange(
            String sessionId, String secretHash, String issuer, Instant now, Instant idleExpiresAt) {
        requireMutationArguments(sessionId, secretHash, issuer, now);
        Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
        Bson filter = Filters.and(
                Filters.eq("_id", sessionId),
                Filters.eq("secretHash", secretHash),
                Filters.eq("issuer", issuer),
                Filters.eq("revoked", false),
                Filters.gt("idleExpiresAt", now.toEpochMilli()),
                Filters.gt("absoluteExpiresAt", now.toEpochMilli()));
        List<Bson> update = List.of(new Document("$set", new Document("lastUsedAt", now.toEpochMilli())
                .append("idleExpiresAt", new Document("$min",
                        List.of("$absoluteExpiresAt", idleExpiresAt.toEpochMilli())))));
        Document touched = StoreIo.call(() -> collection.findOneAndUpdate(filter, update, RETURN_AFTER));
        return touched == null ? Optional.empty() : Optional.of(toRecord(touched));
    }

    @Override
    public boolean revoke(String sessionId, String secretHash, String issuer, Instant now) {
        requireMutationArguments(sessionId, secretHash, issuer, now);
        Bson filter = Filters.and(
                Filters.eq("_id", sessionId),
                Filters.eq("secretHash", secretHash),
                Filters.eq("issuer", issuer),
                Filters.gt("idleExpiresAt", now.toEpochMilli()),
                Filters.gt("absoluteExpiresAt", now.toEpochMilli()));
        Document revoked = StoreIo.call(() -> collection.findOneAndUpdate(
                filter, Updates.set("revoked", true), RETURN_AFTER));
        return revoked != null;
    }

    static Document toDocument(SessionRecord record) {
        return new Document("_id", record.sessionId())
                .append("secretHash", record.secretHash())
                .append("principal", record.principal())
                .append("scope", record.scope())
                .append("issuer", record.issuer())
                .append("revoked", record.revoked())
                .append("createdAt", record.createdAt().toEpochMilli())
                .append("lastUsedAt", record.lastUsedAt().toEpochMilli())
                .append("idleExpiresAt", record.idleExpiresAt().toEpochMilli())
                .append("absoluteExpiresAt", record.absoluteExpiresAt().toEpochMilli());
    }

    static SessionRecord toRecord(Document document) {
        Object id = document.get("_id");
        try {
            String sessionId = document.getString("_id");
            String secretHash = document.getString("secretHash");
            String principal = document.getString("principal");
            String scope = document.getString("scope");
            String issuer = document.getString("issuer");
            Boolean revoked = document.getBoolean("revoked");
            Long createdAt = document.getLong("createdAt");
            Long lastUsedAt = document.getLong("lastUsedAt");
            Long idleExpiresAt = document.getLong("idleExpiresAt");
            Long absoluteExpiresAt = document.getLong("absoluteExpiresAt");
            if (revoked == null || createdAt == null || lastUsedAt == null
                    || idleExpiresAt == null || absoluteExpiresAt == null) {
                throw new IllegalArgumentException("session record is missing a required field");
            }
            return new SessionRecord(sessionId, secretHash, principal, scope, issuer, revoked,
                    Instant.ofEpochMilli(createdAt), Instant.ofEpochMilli(lastUsedAt),
                    Instant.ofEpochMilli(idleExpiresAt), Instant.ofEpochMilli(absoluteExpiresAt));
        } catch (RuntimeException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), invalid);
        }
    }

    private static void requireMutationArguments(
            String sessionId, String secretHash, String issuer, Instant now) {
        Objects.requireNonNull(sessionId, "sessionId");
        Objects.requireNonNull(secretHash, "secretHash");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(now, "now");
    }
}
