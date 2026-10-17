package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteValue;
import org.bson.Document;
import org.bson.types.MinKey;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Configuration-only note CAS, clear generations and persistent shared-carry barriers. */
public final class MongoRequiredNoteStore implements RequiredNoteStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final String ALL_KEYS = "cleared";
    private final MongoClient client;
    private final MongoCollection<Document> scopes;
    private final MongoCollection<Document> notes;
    private final MongoCollection<Document> migrations;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> operators;
    private final SourceConfigCipherProvider ciphers;

    public MongoRequiredNoteStore(MongoClient client, MongoDatabase database, SourceConfigCipher cipher) {
        this(client, database, SourceConfigCipherProvider.fixed(Objects.requireNonNull(cipher, "cipher")));
    }

    MongoRequiredNoteStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        Objects.requireNonNull(database, "database");
        this.scopes = durable(SystemCollections.PDK_NOTE_SCOPES.on(database));
        this.notes = durable(SystemCollections.PDK_NOTES.on(database));
        this.migrations = durable(SystemCollections.PDK_NOTE_MIGRATIONS.on(database));
        this.identity = durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database));
        this.operators = durable(SystemCollections.OPERATOR_GENERATIONS.on(database));
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public RequiredNoteScope initialize(RequiredNoteOwner owner) {
        Objects.requireNonNull(owner, "owner");
        return StoreIo.call(() -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    return transaction(session -> {
                        cipherForWrite(session);
                        checkOwner(session, owner, true);
                        Document existing = scopes.find(session, byScope(owner)).first();
                        if (existing == null) {
                            existing = new Document("_id", scopeId(owner)).append("ownerKind", owner.kind().name())
                                    .append("ownerId", owner.ownerId()).append("generation", 1L).append("writeFence", 0L);
                            scopes.insertOne(session, existing);
                        }
                        return scopeOf(owner, existing);
                    });
                } catch (MongoException raced) {
                    if (attempt >= 2 || ErrorCategory.fromErrorCode(raced.getCode()) != ErrorCategory.DUPLICATE_KEY) {
                        throw raced;
                    }
                }
            }
        });
    }

    @Override
    public RequiredNoteScope open(RequiredNoteOwner owner) {
        Objects.requireNonNull(owner, "owner");
        return StoreIo.call(() -> transaction(session -> {
            checkOwner(session, owner, false);
            return scopeOf(owner, scopes.find(session, byScope(owner)).first());
        }));
    }

    @Override
    public RequiredNoteValue read(RequiredNoteScope scope, String key) {
        Objects.requireNonNull(key, "key");
        return StoreIo.call(() -> transaction(session -> {
            verifyScope(session, scope, false);
            return decode(scope, key, notes.find(session, byNote(scope, key)).first());
        }));
    }

    @Override
    public boolean compareAndSet(RequiredNoteScope scope, String key, long expectedRevision, byte[] bytes,
            boolean blockEarlierValue) {
        Objects.requireNonNull(key, "key");
        if (expectedRevision < 0 || (blockEarlierValue && bytes != null)) {
            throw new IllegalArgumentException("invalid required-note write condition");
        }
        byte[] captured = bytes == null ? null : bytes.clone();
        return StoreIo.call(() -> {
            try {
                return transaction(session -> {
                    SourceConfigCipher cipher = cipherForWrite(session);
                    verifyScope(session, scope, true);
                    RequiredNoteValue current = decode(scope, key, notes.find(session, byNote(scope, key)).first());
                    if (current.revision() != expectedRevision) throw new Refused();
                    if (expectedRevision == Long.MAX_VALUE) throw unreadable("revision");
                    long next = expectedRevision + 1;
                    Document replacement = new Document("_id", noteId(scope, key))
                            .append("revision", next).append("deleted", captured == null);
                    if (captured != null) {
                        replacement.append("value", cipher.encrypt(context(scope, key, next), "pdk-notes",
                                Base64.getUrlEncoder().withoutPadding().encodeToString(captured)));
                    }
                    if (current.revision() == 0) {
                        notes.insertOne(session, replacement);
                    } else if (notes.replaceOne(session, byNote(scope, key).append("revision", expectedRevision),
                            replacement).getMatchedCount() != 1) {
                        throw new Refused();
                    }
                    if (blockEarlierValue) block(session, scope.owner(), "removed:" + key);
                    return true;
                });
            } catch (Refused refused) {
                return false;
            }
        });
    }

    @Override
    public boolean carryBlocked(RequiredNoteScope scope, String key) {
        Objects.requireNonNull(key, "key");
        return StoreIo.call(() -> transaction(session -> {
            verifyScope(session, scope, false);
            return blocked(session, scope.owner(), ALL_KEYS) || blocked(session, scope.owner(), "removed:" + key);
        }));
    }

    @Override
    public RequiredNoteScope clear(RequiredNoteScope scope, boolean blockEarlierValues) {
        return StoreIo.call(() -> transaction(session -> {
            cipherForWrite(session);
            verifyScope(session, scope, true);
            if (scope.generation() == Long.MAX_VALUE) throw unreadable("generation");
            if (blockEarlierValues) block(session, scope.owner(), ALL_KEYS);
            long matched = scopes.updateOne(session, byScope(scope.owner()).append("generation", scope.generation()),
                    new Document("$inc", new Document("generation", 1L))).getMatchedCount();
            if (matched != 1) throw unavailable();
            return new RequiredNoteScope(scope.owner(), scope.generation() + 1);
        }));
    }

    @Override
    public long reclaimBefore(RequiredNoteScope scope) {
        Objects.requireNonNull(scope, "scope");
        StoreIo.run(() -> transaction(session -> {
            checkOwner(session, scope.owner(), false);
            RequiredNoteScope current = scopeOf(scope.owner(), scopes.find(session, byScope(scope.owner())).first());
            if (current.generation() < scope.generation()) throw unavailable();
            return null;
        }));
        // The exact captured upper bound remains safe if another clear commits after verification.
        Document low = notePrefix(scope.owner(), Long.MIN_VALUE).append("key", new MinKey());
        Document high = notePrefix(scope.owner(), scope.generation()).append("key", new MinKey());
        return StoreIo.call(() -> notes.deleteMany(new Document("_id",
                new Document("$gte", low).append("$lt", high))).getDeletedCount());
    }

    private <T> T transaction(Function<ClientSession, T> body) {
        try (ClientSession session = client.startSession()) {
            return session.withTransaction(() -> body.apply(session), TRANSACTION);
        }
    }

    private void verifyScope(ClientSession session, RequiredNoteScope scope, boolean write) {
        Objects.requireNonNull(scope, "scope");
        checkOwner(session, scope.owner(), write);
        Document document = scopes.find(session, byScope(scope.owner())).first();
        RequiredNoteScope observed = scopeOf(scope.owner(), document);
        if (observed.generation() != scope.generation()) throw unavailable();
        if (write) {
            if (!(document.get("writeFence") instanceof Long fence) || fence < 0 || fence == Long.MAX_VALUE) {
                throw unreadable("writeFence");
            }
            if (scopes.updateOne(session, byScope(scope.owner()).append("generation", scope.generation()),
                    new Document("$inc", new Document("writeFence", 1L))).getMatchedCount() != 1) throw unavailable();
        }
    }

    private void checkOwner(ClientSession session, RequiredNoteOwner owner, boolean write) {
        Document cluster = identity.find(session, new Document("_id", "cluster")).first();
        if (cluster == null || !MongoConfigurationDomain.LAYOUT.equals(cluster.get("layoutId"))
                || !Long.valueOf(owner.bindingVersion()).equals(cluster.get("bindingVersion"))) throw unavailable();
        if (owner.kind() == RequiredNoteOwner.Kind.GLOBAL && !owner.ownerId().equals(cluster.get("clusterId"))) {
            throw unavailable();
        }
        if (owner.kind() == RequiredNoteOwner.Kind.PIPELINE) {
            Document filter = new Document("_id", owner.ownerId()).append("stateGeneration", owner.stateGeneration());
            if (write) {
                if (operators.updateOne(session, filter,
                        new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) throw unavailable();
            } else if (operators.find(session, filter).first() == null) {
                throw unavailable();
            }
        }
    }

    private SourceConfigCipher cipherForWrite(ClientSession session) {
        SourceConfigCipher cipher = ciphers.currentForWrite();
        if (ciphers.requiresWriteFence()
                && !ciphers.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) {
            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
        }
        return cipher;
    }

    private static RequiredNoteScope scopeOf(RequiredNoteOwner owner, Document document) {
        if (document == null || !owner.kind().name().equals(document.get("ownerKind"))
                || !owner.ownerId().equals(document.get("ownerId"))
                || !(document.get("generation") instanceof Long generation) || generation < 1) throw unavailable();
        return new RequiredNoteScope(owner, generation);
    }

    private RequiredNoteValue decode(RequiredNoteScope scope, String key, Document document) {
        if (document == null) return new RequiredNoteValue(0, null);
        if (!(document.get("revision") instanceof Long revision) || revision < 1
                || !(document.get("deleted") instanceof Boolean deleted)) throw unreadable("revision");
        if (deleted) {
            if (document.containsKey("value")) throw unreadable("value");
            return new RequiredNoteValue(revision, null);
        }
        if (!(document.get("value") instanceof String value)) throw unreadable("value");
        try {
            String plaintext = ciphers.current().decrypt(context(scope, key, revision), "pdk-notes", value);
            return new RequiredNoteValue(revision, Base64.getUrlDecoder().decode(plaintext));
        } catch (TapstateException failure) {
            if (failure.code() != IoError.DOCUMENT_UNREADABLE) throw failure;
            throw unreadable("value");
        } catch (IllegalArgumentException invalidPayload) {
            throw unreadable("value");
        }
    }

    private boolean blocked(ClientSession session, RequiredNoteOwner owner, String key) {
        Document document = migrations.find(session, new Document("_id", markerId(owner, key))).first();
        if (document == null) return false;
        if (!Boolean.TRUE.equals(document.get("blocked"))) throw unreadable("migration");
        return true;
    }

    private void block(ClientSession session, RequiredNoteOwner owner, String key) {
        if (blocked(session, owner, key)) return;
        migrations.updateOne(session, new Document("_id", markerId(owner, key)),
                new Document("$setOnInsert", new Document("blocked", true)), new UpdateOptions().upsert(true));
    }

    private static String context(RequiredNoteScope scope, String key, long revision) {
        return JsonWriter.write(List.of(scope.owner().bindingVersion(), scope.owner().namespace(),
                scope.generation(), key, revision));
    }

    private static Document scopeId(RequiredNoteOwner owner) {
        return new Document("bindingVersion", owner.bindingVersion()).append("namespace", owner.namespace());
    }

    private static Document byScope(RequiredNoteOwner owner) {
        return new Document("_id", scopeId(owner));
    }

    private static Document notePrefix(RequiredNoteOwner owner, long generation) {
        return scopeId(owner).append("generation", generation);
    }

    private static Document noteId(RequiredNoteScope scope, String key) {
        return notePrefix(scope.owner(), scope.generation()).append("key", key);
    }

    private static Document byNote(RequiredNoteScope scope, String key) {
        return new Document("_id", noteId(scope, key));
    }

    private static Document markerId(RequiredNoteOwner owner, String key) {
        return scopeId(owner).append("key", key);
    }

    private static TapstateException unavailable() {
        return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
    }

    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "required-note", "field", field), null);
    }

    private static final class Refused extends RuntimeException {
        private Refused() { super(null, null, false, false); }
    }
}
