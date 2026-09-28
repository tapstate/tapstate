package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Minimal managed login records and early-revocation markers beside the unchanged standalone sessions. */
public final class MongoCloudSessionStore implements CloudSessionStore {

    private static final String ORIGIN = "cloud";
    private static final String DOCUMENT_NAME = "cloud-session";
    private static final byte[] KEY_DOMAIN = "io.tapstate.cloud-session/v1\0".getBytes(StandardCharsets.UTF_8);
    private static final List<String> LOGIN_FIELDS =
            List.of("secretHash", "userId", "scope", "createdAt", "lastUsedAt", "idleExpiresAt");
    private static final FindOneAndUpdateOptions RETURN_AFTER =
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);

    private final MongoCollection<Document> collection;

    public MongoCloudSessionStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public boolean create(CloudSessionRecord record) {
        Objects.requireNonNull(record, "record");
        Document document = toDocument(record);
        return StoreIo.call(DOCUMENT_NAME, () -> {
            try {
                // Insert-only also refuses a deny marker left by a callback before this login write.
                collection.insertOne(document);
                return true;
            } catch (MongoException failure) {
                int code = failure instanceof MongoWriteException write
                        ? write.getError().getCode() : failure.getCode();
                if (ErrorCategory.fromErrorCode(code) == ErrorCategory.DUPLICATE_KEY) {
                    return false;
                }
                throw failure;
            }
        });
    }

    @Override
    public Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jwtId) {
        Binary id = sessionKey(identity, jwtId);
        Document found = StoreIo.call(DOCUMENT_NAME, () -> collection.find(Filters.eq("_id", id)).first());
        return found == null ? Optional.empty() : toRecord(found);
    }

    @Override
    public Optional<CloudSessionRecord> authenticate(
            CloudSessionIdentity identity, String jwtId, String secretHash, Instant now, Instant idleExpiresAt) {
        requireMutationArguments(secretHash, now);
        Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
        Bson filter = Filters.and(contextFilter(identity, jwtId),
                Filters.eq("secretHash", secretHash),
                Filters.eq("revoked", false),
                Filters.gt("idleExpiresAt", now.toEpochMilli()));
        // An older request arriving after a newer one must not shorten the sliding idle deadline.
        Bson update = Updates.combine(
                Updates.max("lastUsedAt", now.toEpochMilli()),
                Updates.max("idleExpiresAt", idleExpiresAt.toEpochMilli()));
        Document touched = StoreIo.call(DOCUMENT_NAME,
                () -> collection.findOneAndUpdate(filter, update, RETURN_AFTER));
        return touched == null ? Optional.empty() : toRecord(touched);
    }

    @Override
    public boolean logout(CloudSessionIdentity identity, String jwtId, String secretHash, Instant now) {
        requireMutationArguments(secretHash, now);
        // Revocation does not depend on the original Cloud JWT expiry or on a still-active idle window.
        Bson filter = Filters.and(contextFilter(identity, jwtId), Filters.eq("secretHash", secretHash));
        Document revoked = StoreIo.call(DOCUMENT_NAME, () -> collection.findOneAndUpdate(
                filter, Updates.set("revoked", true), RETURN_AFTER));
        if (revoked == null) {
            return false;
        }
        return toRecord(revoked).isPresent();
    }

    @Override
    public void invalidate(CloudSessionIdentity identity, String jwtId, Instant now) {
        Objects.requireNonNull(now, "now");
        Binary id = sessionKey(identity, jwtId);
        // Keep this upsert keyed only by the unique _id. Existing login facts are never replaced,
        // and a missing row becomes a permanent deny marker that insert-only creation cannot undo.
        Bson update = Updates.combine(
                Updates.setOnInsert("origin", ORIGIN),
                Updates.setOnInsert("issuer", identity.issuer()),
                Updates.setOnInsert("organizationId", identity.organizationId()),
                Updates.setOnInsert("clusterId", identity.clusterId()),
                Updates.setOnInsert("jwtId", jwtId),
                Updates.set("revoked", true));
        StoreIo.run(DOCUMENT_NAME, () -> collection.updateOne(
                Filters.eq("_id", id), update, new UpdateOptions().upsert(true)));
    }

    static Document toDocument(CloudSessionRecord record) {
        CloudSessionIdentity identity = record.identity();
        return new Document("_id", sessionKey(identity, record.jwtId()))
                .append("origin", ORIGIN)
                .append("issuer", identity.issuer())
                .append("organizationId", identity.organizationId())
                .append("clusterId", identity.clusterId())
                .append("jwtId", record.jwtId())
                .append("secretHash", record.secretHash())
                .append("userId", record.userId())
                .append("scope", record.scope())
                .append("revoked", record.revoked())
                .append("createdAt", record.createdAt().toEpochMilli())
                .append("lastUsedAt", record.lastUsedAt().toEpochMilli())
                .append("idleExpiresAt", record.idleExpiresAt().toEpochMilli());
    }

    static Optional<CloudSessionRecord> toRecord(Document document) {
        if (!ORIGIN.equals(text(document, "origin"))) {
            throw unreadable("origin");
        }
        CloudSessionIdentity identity = new CloudSessionIdentity(
                text(document, "issuer"), text(document, "organizationId"), text(document, "clusterId"));
        String jwtId = text(document, "jwtId");
        if (!sessionKey(identity, jwtId).equals(document.get("_id"))) {
            throw unreadable("_id");
        }
        Object revoked = document.get("revoked");
        if (!(revoked instanceof Boolean)) {
            throw unreadable("revoked");
        }
        boolean hasLogin = LOGIN_FIELDS.stream().anyMatch(document::containsKey);
        if (!hasLogin && Boolean.TRUE.equals(revoked)) {
            return Optional.empty();
        }
        return Optional.of(new CloudSessionRecord(identity, jwtId,
                text(document, "secretHash"), text(document, "userId"), text(document, "scope"),
                (Boolean) revoked, instant(document, "createdAt"), instant(document, "lastUsedAt"),
                instant(document, "idleExpiresAt")));
    }

    static Binary sessionKey(CloudSessionIdentity identity, String jwtId) {
        Objects.requireNonNull(identity, "identity");
        requireText(jwtId, "jwtId");
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
        digest.update(KEY_DOMAIN);
        for (String value : List.of(identity.issuer(), identity.organizationId(), identity.clusterId(), jwtId)) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
        }
        // Binary keys cannot alias the existing standalone session store's String ids.
        return new Binary(digest.digest());
    }

    private static Bson contextFilter(CloudSessionIdentity identity, String jwtId) {
        return Filters.and(Filters.eq("_id", sessionKey(identity, jwtId)), Filters.eq("origin", ORIGIN),
                Filters.eq("issuer", identity.issuer()), Filters.eq("organizationId", identity.organizationId()),
                Filters.eq("clusterId", identity.clusterId()), Filters.eq("jwtId", jwtId));
    }

    private static String text(Document document, String field) {
        Object value = document.get(field);
        if (!(value instanceof String text) || text.isBlank()) {
            throw unreadable(field);
        }
        return text;
    }

    private static Instant instant(Document document, String field) {
        if (!(document.get(field) instanceof Long millis)) {
            throw unreadable(field);
        }
        return Instant.ofEpochMilli(millis);
    }

    private static TapstateException unreadable(String field) {
        // Stored values and reconstruction causes are untrusted and may contain credential material.
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", DOCUMENT_NAME, "field", field), null);
    }

    private static void requireMutationArguments(String secretHash, Instant now) {
        requireText(secretHash, "secretHash");
        Objects.requireNonNull(now, "now");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
    }
}
