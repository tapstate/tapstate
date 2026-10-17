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
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteCarry;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteValue;
import org.bson.Document;
import org.bson.types.MinKey;

import java.util.Base64;
import java.util.ArrayList;
import java.util.HashSet;
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
    private final MongoCollection<Document> carryRows;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> operators;
    private final SourceConfigCipherProvider ciphers;
    private final Runnable requireActive;

    public MongoRequiredNoteStore(MongoClient client, MongoDatabase database, SourceConfigCipher cipher) {
        this(client, database, SourceConfigCipherProvider.fixed(Objects.requireNonNull(cipher, "cipher")));
    }

    MongoRequiredNoteStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        Objects.requireNonNull(database, "database");
        this.scopes = durable(SystemCollections.PDK_NOTE_SCOPES.on(database));
        this.notes = durable(SystemCollections.PDK_NOTES.on(database));
        this.migrations = durable(SystemCollections.PDK_NOTE_MIGRATIONS.on(database));
        this.carryRows = durable(SystemCollections.PDK_NOTE_CARRIES.on(database));
        this.identity = durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database));
        this.operators = durable(SystemCollections.OPERATOR_GENERATIONS.on(database));
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
        this.requireActive = () -> {};
    }

    private MongoRequiredNoteStore(MongoRequiredNoteStore original, Runnable additionalGuard) {
        this.client = original.client;
        this.scopes = original.scopes;
        this.notes = original.notes;
        this.migrations = original.migrations;
        this.carryRows = original.carryRows;
        this.identity = original.identity;
        this.operators = original.operators;
        this.ciphers = original.ciphers;
        this.requireActive = () -> {
            original.requireActive.run();
            additionalGuard.run();
        };
    }

    @Override
    public RequiredNoteStore guardedBy(Runnable additionalGuard) {
        return new MongoRequiredNoteStore(this, Objects.requireNonNull(additionalGuard, "additionalGuard"));
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public RequiredNoteScope initialize(RequiredNoteOwner owner) {
        Objects.requireNonNull(owner, "owner");
        RequiredNoteScope prepared = StoreIo.call(() -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    return transaction(session -> {
                        cipherForWrite(session);
                        checkOwner(session, owner, true);
                        Document existing = scopes.find(session, byScope(owner)).first();
                        if (existing == null) {
                            existing = new Document("_id", scopeId(owner)).append("ownerKind", owner.kind().name())
                                    .append("ownerId", owner.ownerId()).append("generation", 1L).append("writeFence", 0L)
                                    .append("outgoingCarries", 0L).append("incomingCarries", 0L);
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
        if (owner.kind() == RequiredNoteOwner.Kind.CHAIN) {
            StoreIo.run(() -> {
                requireActive.run();
                for (var index : SystemCollections.PDK_NOTE_CARRIES.indexes()) IndexEnsure.createIfAbsent(carryRows, index);
                requireActive.run();
            });
        }
        return prepared;
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
                    requirePrivateUnreferenced(session, scope);
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
            requirePrivateUnreferenced(session, scope);
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
            if (scope.owner().kind() == RequiredNoteOwner.Kind.PIPELINE) {
                Document metadata = scopes.find(session, byScope(scope.owner())).first();
                for (RequiredNoteCarry carry : incoming(session, scope.owner(), metadata)) {
                    if (carry.source().generation() < scope.generation()) throw referenced();
                }
            }
            return null;
        }));
        // The exact captured upper bound remains safe if another clear commits after verification.
        Document low = notePrefix(scope.owner(), Long.MIN_VALUE).append("key", new MinKey());
        Document high = notePrefix(scope.owner(), scope.generation()).append("key", new MinKey());
        return StoreIo.call(() -> {
            requireActive.run();
            return notes.deleteMany(new Document("_id", new Document("$gte", low).append("$lt", high))).getDeletedCount();
        });
    }

    @Override
    public RequiredNoteCarry prepareCarry(RequiredNoteCarry carry) {
        Objects.requireNonNull(carry, "carry");
        return withCarryIndexes(() -> transaction(session -> {
            cipherForWrite(session);
            Document target = verifyScope(session, carry.shared(), true);
            List<RequiredNoteCarry> retained = outgoing(session, carry.shared().owner(), target);
            for (RequiredNoteCarry existing : retained) {
                if (carryId(existing).equals(carryId(carry))) {
                    if (!existing.equals(carry)) throw unavailable();
                    verifyCarrySource(session, existing.source(), false);
                    return existing;
                }
            }
            if (blocked(session, carry.shared().owner(), ALL_KEYS)) throw unavailable();
            Document source = verifyScope(session, carry.source(), true);
            incoming(session, carry.source().owner(), source);
            carryRows.insertOne(session, carryDocument(carry));
            changeCount(session, carry.shared(), "outgoingCarries", carryCount(target, "outgoingCarries"), 1);
            changeCount(session, carry.source(), "incomingCarries", carryCount(source, "incomingCarries"), 1);
            return carry;
        }));
    }

    @Override
    public List<RequiredNoteCarry> carries(RequiredNoteScope shared) {
        requireShared(shared);
        return withCarryIndexes(() -> transaction(session -> {
            Document metadata = verifyScope(session, shared, false);
            List<RequiredNoteCarry> retained = outgoing(session, shared.owner(), metadata);
            if (retained.stream().anyMatch(carry -> carry.shared().generation() > shared.generation())) throw unavailable();
            List<RequiredNoteCarry> current = retained.stream().filter(carry -> carry.shared().equals(shared)).toList();
            for (RequiredNoteCarry carry : current) {
                Document source = verifyCarrySource(session, carry.source(), false);
                if (!incoming(session, carry.source().owner(), source).contains(carry)) throw unavailable();
            }
            return current;
        }));
    }

    @Override
    public RequiredNoteValue readCarried(RequiredNoteCarry carry, String key) {
        Objects.requireNonNull(carry, "carry");
        Objects.requireNonNull(key, "key");
        return withCarryIndexes(() -> transaction(session -> {
            Document metadata = verifyScope(session, carry.shared(), false);
            List<RequiredNoteCarry> retained = outgoing(session, carry.shared().owner(), metadata);
            if (!retained.contains(carry)) throw unavailable();
            if (blocked(session, carry.shared().owner(), ALL_KEYS)
                    || blocked(session, carry.shared().owner(), "removed:" + key)) return new RequiredNoteValue(0, null);
            Document source = verifyCarrySource(session, carry.source(), false);
            if (!incoming(session, carry.source().owner(), source).contains(carry)) throw unavailable();
            return decode(carry.source(), key, notes.find(session, byNote(carry.source(), key)).first());
        }));
    }

    @Override
    public boolean allCarryBlocked(RequiredNoteScope shared) {
        requireShared(shared);
        return StoreIo.call(() -> transaction(session -> {
            verifyScope(session, shared, false);
            return blocked(session, shared.owner(), ALL_KEYS);
        }));
    }

    @Override
    public RequiredNoteScope retireShared(RequiredNoteScope shared) {
        requireShared(shared);
        return withCarryIndexes(() -> transaction(session -> {
            cipherForWrite(session);
            Document metadata = verifyScope(session, shared, true);
            if (shared.generation() == Long.MAX_VALUE) throw unreadable("generation");
            List<RequiredNoteCarry> retained = outgoing(session, shared.owner(), metadata);
            for (RequiredNoteCarry carry : retained) {
                if (carry.shared().generation() > shared.generation()) throw unavailable();
                Document source = verifyCarrySource(session, carry.source(), true);
                if (!incoming(session, carry.source().owner(), source).contains(carry)) throw unavailable();
                if (carryRows.deleteOne(session, new Document("_id", carryId(carry))).getDeletedCount() != 1) throw unavailable();
                changeCount(session, carry.source(), "incomingCarries", carryCount(source, "incomingCarries"), -1);
            }
            block(session, shared.owner(), ALL_KEYS);
            long matched = scopes.updateOne(session, byScope(shared.owner()).append("generation", shared.generation())
                            .append("outgoingCarries", carryCount(metadata, "outgoingCarries")),
                    new Document("$inc", new Document("generation", 1L))
                            .append("$set", new Document("outgoingCarries", 0L))).getMatchedCount();
            if (matched != 1) throw unavailable();
            return new RequiredNoteScope(shared.owner(), shared.generation() + 1);
        }));
    }

    private Document verifyCarrySource(ClientSession session, RequiredNoteScope source, boolean write) {
        checkBinding(session, source.owner());
        Document operator = operators.find(session, new Document("_id", source.owner().ownerId())).first();
        PipelineStateMapping current;
        try {
            current = MongoPipelineStateMappings.decode(source.owner().ownerId(), operator);
        } catch (TapstateException invalidMapping) {
            throw unavailable();
        }
        if (current.phase() == PipelineStateMapping.Phase.RETIRED
                || current.stateGeneration() < source.owner().stateGeneration()) throw unavailable();
        return verifyPhysicalScope(session, source, write);
    }

    private void requirePrivateUnreferenced(ClientSession session, RequiredNoteScope scope) {
        if (scope.owner().kind() != RequiredNoteOwner.Kind.PIPELINE) return;
        Document metadata = scopes.find(session, byScope(scope.owner())).first();
        if (!incoming(session, scope.owner(), metadata).isEmpty()) throw referenced();
    }

    private List<RequiredNoteCarry> outgoing(ClientSession session, RequiredNoteOwner owner, Document metadata) {
        return dependencies(session, new Document("_id.sharedBindingVersion", owner.bindingVersion())
                .append("_id.sharedNamespace", owner.namespace()), metadata, "outgoingCarries", owner, true);
    }

    private List<RequiredNoteCarry> incoming(ClientSession session, RequiredNoteOwner owner, Document metadata) {
        return dependencies(session, new Document("_id.sourceBindingVersion", owner.bindingVersion())
                .append("_id.sourceNamespace", owner.namespace()), metadata, "incomingCarries", owner, false);
    }

    private List<RequiredNoteCarry> dependencies(ClientSession session, Document filter, Document metadata,
            String countField, RequiredNoteOwner owner, boolean target) {
        scopeOf(owner, metadata);
        List<RequiredNoteCarry> result = new ArrayList<>();
        try (var cursor = carryRows.find(session, filter).iterator()) {
            while (cursor.hasNext()) {
                RequiredNoteCarry carry = decodeCarry(cursor.next());
                RequiredNoteOwner endpoint = (target ? carry.shared() : carry.source()).owner();
                if (endpoint.bindingVersion() != owner.bindingVersion() || !endpoint.namespace().equals(owner.namespace())
                        || endpoint.kind() != owner.kind() || !endpoint.ownerId().equals(owner.ownerId())) throw unavailable();
                result.add(carry);
            }
        }
        if (carryCount(metadata, countField) != result.size() || new HashSet<>(result.stream().map(MongoRequiredNoteStore::carryId).toList()).size() != result.size()) {
            throw unreadable("carryCount");
        }
        return List.copyOf(result);
    }

    private void changeCount(ClientSession session, RequiredNoteScope scope, String field, long expected, int delta) {
        if ((delta == 1 && expected == Long.MAX_VALUE - 1) || expected + delta < 0) throw unreadable("carryCount");
        if (scopes.updateOne(session, byScope(scope.owner()).append("generation", scope.generation()).append(field, expected),
                new Document("$inc", new Document(field, (long) delta))).getMatchedCount() != 1) throw unavailable();
    }

    private <T> T withCarryIndexes(java.util.function.Supplier<T> body) {
        return StoreIo.call(() -> {
            requireActive.run();
            for (var declared : SystemCollections.PDK_NOTE_CARRIES.indexes()) {
                Document found = IndexEnsure.existing(carryRows, declared);
                Document keys = new Document();
                declared.keys().forEach(key -> keys.append(key, 1));
                if (found == null || !keys.equals(found.get("key"))
                        || found.getBoolean("unique", false) != declared.unique()) throw unreadable("carryIndex");
            }
            return body.get();
        });
    }

    private static long carryCount(Document document, String field) {
        if (document == null || !(document.get(field) instanceof Long count) || count < 0 || count == Long.MAX_VALUE) {
            throw unreadable("carryCount");
        }
        return count;
    }

    private static void requireShared(RequiredNoteScope scope) {
        Objects.requireNonNull(scope, "scope");
        if (scope.owner().kind() != RequiredNoteOwner.Kind.CHAIN) throw new IllegalArgumentException("a shared chain scope is required");
    }

    private static Document carryId(RequiredNoteCarry carry) {
        return new Document("sharedBindingVersion", carry.shared().owner().bindingVersion())
                .append("sharedNamespace", carry.shared().owner().namespace()).append("sharedGeneration", carry.shared().generation())
                .append("sourceBindingVersion", carry.source().owner().bindingVersion())
                .append("sourceNamespace", carry.source().owner().namespace()).append("sourceGeneration", carry.source().generation());
    }

    private static Document carryDocument(RequiredNoteCarry carry) {
        return new Document("_id", carryId(carry)).append("sharedOwnerId", carry.shared().owner().ownerId())
                .append("sourceOwnerId", carry.source().owner().ownerId()).append("sourceStateGeneration", carry.source().owner().stateGeneration());
    }

    private static RequiredNoteCarry decodeCarry(Document document) {
        try {
            Document id = document.get("_id", Document.class);
            var shared = new RequiredNoteScope(new RequiredNoteOwner(id.getLong("sharedBindingVersion"), id.getString("sharedNamespace"),
                    RequiredNoteOwner.Kind.CHAIN, document.getString("sharedOwnerId"), 0), id.getLong("sharedGeneration"));
            var source = new RequiredNoteScope(new RequiredNoteOwner(id.getLong("sourceBindingVersion"), id.getString("sourceNamespace"),
                    RequiredNoteOwner.Kind.PIPELINE, document.getString("sourceOwnerId"), document.getLong("sourceStateGeneration")), id.getLong("sourceGeneration"));
            return new RequiredNoteCarry(shared, source);
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) {
            throw unreadable("carry");
        }
    }

    private static TapstateException referenced() {
        return new TapstateException(IoError.NOTE_REFERENCED, Map.of(), null);
    }

    private <T> T transaction(Function<ClientSession, T> body) {
        try (ClientSession session = client.startSession()) {
            return session.withTransaction(() -> {
                requireActive.run();
                T result = body.apply(session);
                requireActive.run();
                return result;
            }, TRANSACTION);
        }
    }

    private Document verifyScope(ClientSession session, RequiredNoteScope scope, boolean write) {
        Objects.requireNonNull(scope, "scope");
        checkOwner(session, scope.owner(), write);
        return verifyPhysicalScope(session, scope, write);
    }

    private Document verifyPhysicalScope(ClientSession session, RequiredNoteScope scope, boolean write) {
        Document document = scopes.find(session, byScope(scope.owner())).first();
        RequiredNoteScope observed = scopeOf(scope.owner(), document);
        if (observed.generation() != scope.generation()) throw unavailable();
        if (write) {
            if (scopes.updateOne(session, byScope(scope.owner()).append("generation", scope.generation()),
                    new Document("$inc", new Document("writeFence", 1L))).getMatchedCount() != 1) throw unavailable();
        }
        return document;
    }

    private void checkBinding(ClientSession session, RequiredNoteOwner owner) {
        Document cluster = identity.find(session, new Document("_id", "cluster")).first();
        if (cluster == null || !MongoConfigurationDomain.LAYOUT.equals(cluster.get("layoutId"))
                || !Long.valueOf(owner.bindingVersion()).equals(cluster.get("bindingVersion"))) throw unavailable();
        if (owner.kind() == RequiredNoteOwner.Kind.GLOBAL && !owner.ownerId().equals(cluster.get("clusterId"))) {
            throw unavailable();
        }
    }

    private void checkOwner(ClientSession session, RequiredNoteOwner owner, boolean write) {
        checkBinding(session, owner);
        if (owner.kind() == RequiredNoteOwner.Kind.PIPELINE) {
            Document operator = operators.find(session, new Document("_id", owner.ownerId())).first();
            PipelineStateMapping mapping;
            try {
                mapping = MongoPipelineStateMappings.decode(owner.ownerId(), operator);
            } catch (TapstateException invalidMapping) {
                throw unavailable();
            }
            if (mapping.phase() != PipelineStateMapping.Phase.ACTIVE || mapping.stateGeneration() != owner.stateGeneration()) {
                throw unavailable();
            }
            if (write) {
                Document filter = new Document("_id", owner.ownerId()).append("stateGeneration", owner.stateGeneration())
                        .append("phase", "ACTIVE").append("noteWriteFence", operator.getLong("noteWriteFence"));
                if (operators.updateOne(session, filter,
                        new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) throw unavailable();
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
        if (!(document.get("writeFence") instanceof Long fence) || fence < 0 || fence == Long.MAX_VALUE) {
            throw unreadable("writeFence");
        }
        long outgoing = carryCount(document, "outgoingCarries");
        long incoming = carryCount(document, "incomingCarries");
        if ((owner.kind() != RequiredNoteOwner.Kind.CHAIN && outgoing != 0)
                || (owner.kind() != RequiredNoteOwner.Kind.PIPELINE && incoming != 0)) throw unreadable("carryCount");
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
