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
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteOrigin;
import io.tapstate.spi.store.RequiredNoteCarry;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteRetirement;
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
    private final MongoCollection<Document> origins;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> operators;
    private final SourceConfigCipherProvider ciphers;
    private final Runnable requireActive;
    private final Retirement retirement;
    private record Retirement(RequiredNoteScope scope, String operationId) {}

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
        this.origins = durable(SystemCollections.PDK_NOTE_ORIGINS.on(database));
        this.identity = durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database));
        this.operators = durable(SystemCollections.OPERATOR_GENERATIONS.on(database));
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
        this.requireActive = () -> {};
        this.retirement = null;
    }

    private MongoRequiredNoteStore(MongoRequiredNoteStore original, Runnable additionalGuard) {
        this(original, additionalGuard, original.retirement);
    }

    private MongoRequiredNoteStore(MongoRequiredNoteStore original, Runnable additionalGuard, Retirement retirement) {
        this.retirement = retirement;
        this.client = original.client;
        this.scopes = original.scopes;
        this.notes = original.notes;
        this.migrations = original.migrations;
        this.carryRows = original.carryRows;
        this.origins = original.origins;
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

    /** Controlled installation prepares declared indexes before any private connector scope is published. */
    static void prepareIndexes(MongoDatabase database) {
        StoreIo.run(() -> {
            for (var table : List.of(SystemCollections.PDK_NOTE_ORIGINS, SystemCollections.PDK_NOTE_CARRIES)) {
                for (var index : table.indexes()) IndexEnsure.createIfAbsent(table.on(database), index);
            }
        });
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public RequiredNoteScope initialize(RequiredNoteOwner owner) {
        requireOrdinary();
        Objects.requireNonNull(owner, "owner");
        RequiredNoteScope prepared = StoreIo.call(() -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    return transaction(session -> initialize(session, owner));
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
        if (owner.kind() != RequiredNoteOwner.Kind.GLOBAL) {
            StoreIo.run(() -> {
                requireActive.run();
                for (var index : SystemCollections.PDK_NOTE_ORIGINS.indexes()) IndexEnsure.createIfAbsent(origins, index);
                requireActive.run();
            });
        }
        return prepared;
    }

    /** Authoring reserves new identities inside its existing configuration transaction. */
    RequiredNoteScope initialize(ClientSession session, RequiredNoteOwner owner) {
        cipherForWrite(session);
        checkOwner(session, owner, true);
        Document existing = scopes.find(session, byScope(owner)).first();
        if (existing == null) {
            refuseRetainedFacts(session, owner);
            existing = new Document("_id", scopeId(owner)).append("ownerKind", owner.kind().name())
                    .append("ownerId", owner.ownerId()).append("generation", 1L).append("writeFence", 0L)
                    .append("outgoingCarries", 0L).append("incomingCarries", 0L).append("originGeneration", 0L);
            scopes.insertOne(session, existing);
        }
        return scopeOf(owner, existing);
    }

    private void refuseRetainedFacts(ClientSession session, RequiredNoteOwner owner) {
        Document prefix = new Document("_id.bindingVersion", owner.bindingVersion())
                .append("_id.namespace", owner.namespace());
        if (notes.find(session, prefix).projection(new Document("_id", 1)).first() != null
                || origins.find(session, prefix).projection(new Document("_id", 1)).first() != null
                || migrations.find(session, prefix).projection(new Document("_id", 1)).first() != null
                || carryRows.find(session, new Document("$or", List.of(
                        new Document("_id.sourceBindingVersion", owner.bindingVersion())
                                .append("_id.sourceNamespace", owner.namespace()),
                        new Document("_id.sharedBindingVersion", owner.bindingVersion())
                                .append("_id.sharedNamespace", owner.namespace()))))
                        .projection(new Document("_id", 1)).first() != null) {
            throw unavailable();
        }
    }

    RequiredNoteScope open(ClientSession session, RequiredNoteOwner owner) {
        checkOwner(session, owner, false);
        return scopeOf(owner, scopes.find(session, byScope(owner)).first());
    }

    @Override
    public RequiredNoteScope open(RequiredNoteOwner owner) {
        Objects.requireNonNull(owner, "owner");
        return StoreIo.call(() -> transaction(session -> open(session, owner)));
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
    public RequiredNoteOrigin prepareOrigin(RequiredNoteScope scope, RequiredNoteOrigin origin) {
        requireOrdinary();
        requireOriginScope(scope);
        Objects.requireNonNull(origin, "origin");
        return withOriginIndexes(() -> transaction(session -> {
            SourceConfigCipher cipher = cipherForWrite(session);
            Document metadata = verifyScope(session, scope, true);
            RequiredNoteOrigin retained = retainedOrigin(session, scope, metadata);
            if (retained != null) {
                if (!retained.equals(origin)) throw originMismatch();
                return retained;
            }
            if (notes.find(session, new Document("_id.bindingVersion", scope.owner().bindingVersion())
                    .append("_id.namespace", scope.owner().namespace()).append("_id.generation", scope.generation())).first() != null) {
                throw unavailable();
            }
            origins.insertOne(session, originDocument(scope, origin, cipher));
            if (scopes.updateOne(session, byScope(scope.owner()).append("generation", scope.generation())
                            .append("originGeneration", 0L),
                    new Document("$set", new Document("originGeneration", scope.generation()))).getMatchedCount() != 1) throw unavailable();
            return origin;
        }));
    }

    @Override
    public RequiredNoteOrigin origin(RequiredNoteScope scope) {
        requireOriginScope(scope);
        return withOriginIndexes(() -> transaction(session -> {
            RequiredNoteOrigin retained = retainedOrigin(session, scope, verifyScope(session, scope, false));
            if (retained == null) throw unavailable();
            return retained;
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
        requireOrdinary();
        return StoreIo.call(() -> transaction(session -> {
            SourceConfigCipher cipher = cipherForWrite(session);
            Document metadata = verifyScope(session, scope, true);
            requirePrivateUnreferenced(session, scope);
            if (scope.generation() == Long.MAX_VALUE) throw unreadable("generation");
            RequiredNoteOrigin origin = scope.owner().kind() == RequiredNoteOwner.Kind.GLOBAL ? null
                    : retainedOrigin(session, scope, metadata);
            if (scope.owner().kind() != RequiredNoteOwner.Kind.GLOBAL && origin == null) throw unavailable();
            RequiredNoteScope next = new RequiredNoteScope(scope.owner(), scope.generation() + 1);
            if (origin != null && origins.replaceOne(session, byScope(scope.owner()).append("generation", scope.generation()),
                    originDocument(next, origin, cipher)).getMatchedCount() != 1) throw unavailable();
            if (blockEarlierValues) block(session, scope.owner(), ALL_KEYS);
            long matched = scopes.updateOne(session, byScope(scope.owner()).append("generation", scope.generation()),
                    new Document("$inc", new Document("generation", 1L))
                            .append("$set", new Document("originGeneration", origin == null ? 0L : next.generation()))).getMatchedCount();
            if (matched != 1) throw unavailable();
            return new RequiredNoteScope(scope.owner(), scope.generation() + 1);
        }));
    }

    @Override
    public long reclaimBefore(RequiredNoteScope scope) {
        requireOrdinary();
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
        requireOrdinary();
        Objects.requireNonNull(carry, "carry");
        return withCarryIndexes(() -> transaction(session -> {
            cipherForWrite(session);
            Document target = verifyScope(session, carry.shared(), true);
            List<RequiredNoteCarry> retained = outgoing(session, carry.shared().owner(), target);
            for (RequiredNoteCarry existing : retained) {
                if (carryId(existing).equals(carryId(carry))) {
                    if (!existing.equals(carry)) throw unavailable();
                    Document source = verifyCarrySource(session, existing.source(), false);
                    compatibleOrigins(session, carry.shared(), target, existing.source(), source);
                    return existing;
                }
            }
            if (blocked(session, carry.shared().owner(), ALL_KEYS)) throw unavailable();
            Document source = verifyScope(session, carry.source(), true);
            incoming(session, carry.source().owner(), source);
            compatibleOrigins(session, carry.shared(), target, carry.source(), source);
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
                compatibleOrigins(session, shared, metadata, carry.source(), source);
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
            compatibleOrigins(session, carry.shared(), metadata, carry.source(), source);
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
        requireOrdinary();
        requireShared(shared);
        return withCarryIndexes(() -> transaction(session -> {
            cipherForWrite(session);
            Document metadata = verifyScope(session, shared, true);
            RequiredNoteOrigin origin = retainedOrigin(session, shared, metadata);
            if (origin == null) throw unavailable();
            if (shared.generation() == Long.MAX_VALUE) throw unreadable("generation");
            List<RequiredNoteCarry> retained = outgoing(session, shared.owner(), metadata);
            for (RequiredNoteCarry carry : retained) {
                if (carry.shared().generation() > shared.generation()) throw unavailable();
                Document source = verifyCarrySource(session, carry.source(), true);
                if (!incoming(session, carry.source().owner(), source).contains(carry)) throw unavailable();
                if (carryRows.deleteOne(session, new Document("_id", carryId(carry))).getDeletedCount() != 1) throw unavailable();
                changeCount(session, carry.source(), "incomingCarries", carryCount(source, "incomingCarries"), -1);
            }
            if (origin != null && origins.deleteOne(session, byScope(shared.owner())
                    .append("generation", shared.generation())).getDeletedCount() != 1) throw unavailable();
            block(session, shared.owner(), ALL_KEYS);
            long matched = scopes.updateOne(session, byScope(shared.owner()).append("generation", shared.generation())
                            .append("outgoingCarries", carryCount(metadata, "outgoingCarries")),
                    new Document("$inc", new Document("generation", 1L))
                            .append("$set", new Document("outgoingCarries", 0L).append("originGeneration", 0L))).getMatchedCount();
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

    @Override
    public RequiredNoteRetirement retirement(RequiredNoteScope original, String operationId) {
        requireOrdinary(); Objects.requireNonNull(original, "original");
        if (original.owner().kind() != RequiredNoteOwner.Kind.PIPELINE || operationId == null || operationId.isBlank()) throw unavailable();
        var fixed = new Retirement(original, operationId);
        var view = new MongoRequiredNoteStore(this, () -> {}, fixed);
        withOriginIndexes(() -> withCarryIndexes(() -> transaction(session -> {
            checkRetiringOwner(session, fixed, false);
            Document metadata = scopes.find(session, byScope(original.owner())).first();
            if (retiredResult(fixed, metadata) == null) checkRetirementToken(session, fixed);
            return null;
        })));
        return new RequiredNoteRetirement() {
            @Override public RequiredNoteScope scope() { return original; }
            @Override public String operationId() { return operationId; }
            @Override public RequiredNoteStore notes() { return view; }
            @Override public java.util.Optional<RequiredNoteOrigin> origin() {
                return view.withOriginIndexes(() -> view.transaction(session -> java.util.Optional.ofNullable(
                        view.retainedOrigin(session, original, view.verifyPhysicalScope(session, original, false)))));
            }
            @Override public RequiredNoteScope completeAfterVerifiedRelease() { return completePrivate(fixed); }
        };
    }

    /** The completed result is checked before trying to reuse the now-retired private token. */
    private RequiredNoteScope completePrivate(Retirement fixed) {
        return withOriginIndexes(() -> withCarryIndexes(() -> transaction(session -> {
            checkRetiringOwner(session, fixed, false);
            var original = fixed.scope();
            Document metadata = scopes.find(session, byScope(original.owner())).first();
            RequiredNoteScope completed = retiredResult(fixed, metadata);
            if (completed != null) return completed;
            checkRetirementToken(session, fixed);
            if (original.generation() >= Long.MAX_VALUE - 1) throw unreadable("generation");
            cipherForWrite(session); checkRetiringOwner(session, fixed, true);
            verifyPhysicalScope(session, original, true);
            long next = original.generation() + 1;
            Document result = new Document("operationId", fixed.operationId())
                    .append("stateGeneration", original.owner().stateGeneration())
                    .append("fromGeneration", original.generation()).append("toGeneration", next);
            Document low = notePrefix(original.owner(), Long.MIN_VALUE).append("key", new MinKey());
            Document high = notePrefix(original.owner(), next).append("key", new MinKey());
            notes.deleteMany(session, new Document("_id", new Document("$gte", low).append("$lt", high)));
            origins.deleteOne(session, byScope(original.owner()).append("generation", original.generation()));
            if (scopes.updateOne(session, byScope(original.owner()).append("generation", original.generation()),
                    new Document("$set", new Document("generation", next).append("originGeneration", 0L)
                            .append("lastRetirement", result))).getMatchedCount() != 1) throw unavailable();
            return new RequiredNoteScope(original.owner(), next);
        })));
    }

    private RequiredNoteScope retiredResult(Retirement fixed, Document metadata) {
        var original = fixed.scope(); scopeOf(original.owner(), metadata);
        if (metadata.get("lastRetirement") instanceof Document result && fixed.operationId().equals(result.get("operationId"))) {
            if (!Long.valueOf(original.owner().stateGeneration()).equals(result.get("stateGeneration"))
                    || !Long.valueOf(original.generation()).equals(result.get("fromGeneration"))) throw unavailable();
            return new RequiredNoteScope(original.owner(), result.getLong("toGeneration"));
        }
        return null;
    }

    private void requireOrdinary() { if (retirement != null) throw unavailable(); }

    private void checkRetirementToken(ClientSession session, Retirement fixed) {
        Document metadata = verifyPhysicalScope(session, fixed.scope(), false);
        requirePrivateUnreferenced(session, fixed.scope());
        RequiredNoteOrigin origin = retainedOrigin(session, fixed.scope(), metadata);
        if (origin == null && notes.find(session, new Document("_id.bindingVersion", fixed.scope().owner().bindingVersion())
                .append("_id.namespace", fixed.scope().owner().namespace())).projection(new Document("_id", 1)).first() != null)
            throw unavailable();
    }

    /** Every retirement mutation, including GLOBAL writes, competes with completion on this exact row. */
    private void checkRetiringOwner(ClientSession session, Retirement fixed, boolean write) {
        var owner = fixed.scope().owner(); checkBinding(session, owner);
        Document operator = operators.find(session, new Document("_id", owner.ownerId())).first();
        PipelineStateMapping mapping = MongoPipelineStateMappings.decode(owner.ownerId(), operator);
        if (mapping.stateGeneration() != owner.stateGeneration()
                || (mapping.phase() != PipelineStateMapping.Phase.REBUILDING && mapping.phase() != PipelineStateMapping.Phase.RETIRING)
                || !fixed.operationId().equals(mapping.operationId())) throw unavailable();
        if (write) {
            long fence = operator.getLong("noteWriteFence"); if (fence >= Long.MAX_VALUE - 1) throw unreadable("noteWriteFence");
            if (operators.updateOne(session, new Document("_id", owner.ownerId()).append("stateGeneration", owner.stateGeneration())
                            .append("phase", mapping.phase().name()).append("operationId", fixed.operationId()).append("noteWriteFence", fence),
                    new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) throw unavailable();
        }
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

    private RequiredNoteOrigin retainedOrigin(ClientSession session, RequiredNoteScope scope, Document metadata) {
        scopeOf(scope.owner(), metadata);
        Document stored = origins.find(session, byScope(scope.owner())).first();
        if (Long.valueOf(0).equals(metadata.get("originGeneration"))) {
            if (stored != null) throw unreadable("origin");
            return null;
        }
        if (stored == null || !Long.valueOf(scope.generation()).equals(stored.get("generation"))
                || !(stored.get("value") instanceof String value)) throw unreadable("origin");
        try {
            Object parsed = JsonReader.parse(ciphers.current().decrypt(originContext(scope), "pdk-note-origin", value));
            if (!(parsed instanceof Map<?, ?> tree) || !Long.valueOf(1).equals(tree.get("version"))) throw unreadable("origin");
            return new RequiredNoteOrigin((String) tree.get("connectorId"), (String) tree.get("artifactHash"),
                    Base64.getUrlDecoder().decode((String) tree.get("settings")));
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) {
            throw unreadable("origin");
        } catch (TapstateException invalid) {
            if (invalid.code() != IoError.DOCUMENT_UNREADABLE) throw invalid;
            throw unreadable("origin");
        }
    }

    private static Document originDocument(RequiredNoteScope scope, RequiredNoteOrigin origin, SourceConfigCipher cipher) {
        String body = JsonWriter.write(Map.of("version", 1, "connectorId", origin.connectorId(), "artifactHash", origin.artifactHash(),
                "settings", Base64.getUrlEncoder().withoutPadding().encodeToString(origin.settings())));
        return new Document("_id", scopeId(scope.owner())).append("generation", scope.generation())
                .append("value", cipher.encrypt(originContext(scope), "pdk-note-origin", body));
    }

    private void compatibleOrigins(ClientSession session, RequiredNoteScope shared, Document sharedMetadata,
            RequiredNoteScope source, Document sourceMetadata) {
        RequiredNoteOrigin target = retainedOrigin(session, shared, sharedMetadata);
        RequiredNoteOrigin earlier = retainedOrigin(session, source, sourceMetadata);
        if (target == null || earlier == null) throw unavailable();
        if (!target.equals(earlier)) throw originMismatch();
    }

    private <T> T withOriginIndexes(java.util.function.Supplier<T> body) {
        return StoreIo.call(() -> {
            requireActive.run();
            for (var declared : SystemCollections.PDK_NOTE_ORIGINS.indexes()) {
                Document found = IndexEnsure.existing(origins, declared);
                Document keys = new Document();
                declared.keys().forEach(key -> keys.append(key, 1));
                if (found == null || !keys.equals(found.get("key")) || !Boolean.TRUE.equals(found.get("unique"))) throw unreadable("originIndex");
            }
            return body.get();
        });
    }

    private static String originContext(RequiredNoteScope scope) {
        return JsonWriter.write(List.of(scope.owner().bindingVersion(), scope.owner().namespace(), scope.generation()));
    }

    private static void requireOriginScope(RequiredNoteScope scope) {
        Objects.requireNonNull(scope, "scope");
        if (scope.owner().kind() == RequiredNoteOwner.Kind.GLOBAL) throw new IllegalArgumentException("global notes have no single connector origin");
    }

    private static TapstateException originMismatch() {
        return new TapstateException(IoError.NOTE_ORIGIN_MISMATCH, Map.of(), null);
    }

    private static TapstateException referenced() {
        return new TapstateException(IoError.NOTE_REFERENCED, Map.of(), null);
    }

    private <T> T transaction(Function<ClientSession, T> body) {
        try (ClientSession session = client.startSession()) {
            return session.withTransaction(() -> {
                requireActive.run();
                if (retirement != null) {
                    checkRetiringOwner(session, retirement, false);
                    checkRetirementToken(session, retirement);
                }
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
            if (document.getLong("writeFence") >= Long.MAX_VALUE - 1) throw unreadable("writeFence");
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
        if (retirement != null) {
            if (owner.kind() != RequiredNoteOwner.Kind.GLOBAL && !retirement.scope().owner().equals(owner)) throw unavailable();
            checkRetiringOwner(session, retirement, write);
            checkRetirementToken(session, retirement);
            return;
        }
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
        if (!(document.get("originGeneration") instanceof Long originGeneration)
                || (originGeneration != 0 && originGeneration != generation)
                || (owner.kind() == RequiredNoteOwner.Kind.GLOBAL && originGeneration != 0)) throw unreadable("originGeneration");
        if (document.containsKey("lastRetirement")) {
            if (!(document.get("lastRetirement") instanceof Document result)
                    || !result.keySet().equals(java.util.Set.of("operationId", "stateGeneration", "fromGeneration", "toGeneration"))
                    || !(result.get("operationId") instanceof String operation) || operation.isBlank()
                    || !(result.get("stateGeneration") instanceof Long state) || state < 1 || state == Long.MAX_VALUE
                    || !(result.get("fromGeneration") instanceof Long from) || from < 1 || from >= Long.MAX_VALUE - 1
                    || !(result.get("toGeneration") instanceof Long to) || to != from + 1 || to > generation
                    || owner.kind() != RequiredNoteOwner.Kind.PIPELINE) throw unreadable("lastRetirement");
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
