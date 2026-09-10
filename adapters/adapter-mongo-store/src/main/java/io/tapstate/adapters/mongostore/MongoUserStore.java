package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB user store: each user is stored as one document keyed by its username ({@code _id}), so
 * the username is naturally unique and a save upserts in place — re-saving the same username replaces
 * the record rather than accumulating documents. The password hash and role are stored as plain
 * fields; the raw password is never persisted (hashing happens above the store).
 *
 * <p>A driver IO failure during save / find is translated into a coded io diagnostic through {@link
 * StoreIo}, so no driver type escapes the module (rule R3). A stored document that cannot be read back
 * into a valid user — a missing or blank required field — is surfaced as an io diagnostic rather than
 * a bare crash or a leaked authoring code.
 */
public final class MongoUserStore implements UserStore {

    private final MongoCollection<Document> collection;

    public MongoUserStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(User user) {
        Objects.requireNonNull(user, "user");
        // Upsert by username (the document _id): a re-save of the same username replaces in place.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", user.username()),
                toDocument(user),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<User> find(String username) {
        Objects.requireNonNull(username, "username");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", username)).first());
        return document == null ? Optional.empty() : Optional.of(toUser(document));
    }

    @Override
    public boolean isEmpty() {
        // An existence check, not a count: fetch at most one document rather than scanning the whole
        // collection. Reads no field, so a corrupt document cannot fail this the way find / toUser can.
        Document any = StoreIo.call(() -> collection.find().limit(1).first());
        return any == null;
    }

    /** Maps a user to its stored document, keyed by the username as {@code _id} for natural uniqueness. */
    static Document toDocument(User user) {
        return new Document("_id", user.username())
                .append("passwordHash", user.passwordHash())
                .append("role", user.role());
    }

    /** Reconstructs a user from its stored document; an absent or invalid field is a storage fault. */
    static User toUser(Document document) {
        String username = document.getString("_id");
        String passwordHash = document.getString("passwordHash");
        String role = document.getString("role");
        try {
            return new User(username, passwordHash, role);
        } catch (RuntimeException e) {
            // A stored document whose fields no longer form a valid user (a field is missing or blank)
            // is a storage-integrity failure, surfaced as an io diagnostic — with the original failure
            // kept as the cause — rather than crashing bare from the record's own validation.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(username), "field", "user"), e);
        }
    }
}
