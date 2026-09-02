package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB token store: each machine token is stored as one document keyed by its token id
 * ({@code _id}). The scope, secret hash, revoked flag and creation time are plain fields (the time as
 * epoch millis, a plain long — no JSR-310 codec, matching the audit and state stores); the raw secret
 * is never persisted (hashing happens above the store). Revocation sets the flag in place rather than
 * deleting, so a revoked token still lists and keeps its audit history, and re-revoking is a no-op.
 *
 * <p>A driver IO failure is translated into a coded io diagnostic through {@link StoreIo}, so no
 * driver type escapes the module (rule R3). A stored document that cannot be read back into a valid
 * token record — a missing or invalid required field — is surfaced as an io diagnostic rather than a
 * bare crash or a leaked authoring code.
 */
public final class MongoTokenStore implements TokenStore {

    private final MongoCollection<Document> collection;

    public MongoTokenStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(TokenRecord record) {
        Objects.requireNonNull(record, "record");
        // Upsert by token id (the document _id); a token id is freshly minted, so this inserts a new one.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", record.tokenId()),
                toDocument(record),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<TokenRecord> find(String tokenId) {
        Objects.requireNonNull(tokenId, "tokenId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", tokenId)).first());
        return document == null ? Optional.empty() : Optional.of(toRecord(document));
    }

    @Override
    public void revoke(String tokenId) {
        Objects.requireNonNull(tokenId, "tokenId");
        // Idempotent: an update matching no document (unknown id) is a no-op, and re-revoking just
        // re-sets the flag the record already carries.
        StoreIo.run(() -> collection.updateOne(new Document("_id", tokenId), Updates.set("revoked", true)));
    }

    @Override
    public List<TokenRecord> list() {
        return StoreIo.call(() -> {
            List<TokenRecord> out = new ArrayList<>();
            for (Document document : collection.find()) {
                out.add(toRecord(document));
            }
            return List.copyOf(out);
        });
    }

    /** Maps a token record to its stored document, keyed by the token id as {@code _id}. */
    static Document toDocument(TokenRecord record) {
        return new Document("_id", record.tokenId())
                .append("scope", record.scope())
                .append("secretHash", record.secretHash())
                .append("revoked", record.revoked())
                .append("createdAt", record.createdAt().toEpochMilli());
    }

    /** Reconstructs a token record from its stored document; an absent, blank or wrong-type field is a storage fault. */
    static TokenRecord toRecord(Document document) {
        // The id is captured type-agnostically first so it can still label the diagnostic even when a
        // field is the wrong type; every typed read then happens inside the guard, so a wrong-type
        // field (createdAt as a string, say) surfaces as a coded io diagnostic rather than a bare
        // ClassCastException escaping the module.
        Object id = document.get("_id");
        try {
            String tokenId = document.getString("_id");
            String scope = document.getString("scope");
            String secretHash = document.getString("secretHash");
            Boolean revoked = document.getBoolean("revoked");
            Long createdAt = document.getLong("createdAt");
            if (revoked == null || createdAt == null) {
                // A required field is absent; fail closed (an absent revoked flag must not read as valid).
                throw new IllegalArgumentException("token record is missing a required field");
            }
            return new TokenRecord(tokenId, scope, secretHash, revoked, Instant.ofEpochMilli(createdAt));
        } catch (RuntimeException invalid) {
            // A stored document whose fields no longer form a valid token record — absent, blank, or the
            // wrong type — is a storage-integrity failure, surfaced as an io diagnostic (the original
            // failure kept as the cause) rather than crashing bare from a cast or the record's validation.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "token"), invalid);
        }
    }
}
