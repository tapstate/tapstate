package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Consumer;

/** Cache identity and schema facts only; no configuration key, user, membership or business state is opened. */
final class MongoCacheIdentity {
    static final int AUTH_VERSION = 1;
    static final String PREPARING = "PREPARING";
    static final String READY = "READY";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final TransactionOptions PREPARATION = TransactionOptions.builder()
            .readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> schemas;
    private final CacheAuthScope expected;

    MongoCacheIdentity(MongoClient client, MongoDatabase database, CacheAuthScope expected) {
        this.client = Objects.requireNonNull(client, "client");
        this.database = Objects.requireNonNull(database, "database");
        this.identity = SystemCollections.CACHE_IDENTITY.on(database);
        this.schemas = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database);
        this.expected = Objects.requireNonNull(expected, "expected");
    }

    CacheAuthScope scope() { return expected; }

    void verifyValidationMaintenance(io.tapstate.spi.store.CacheBusinessScope business, String operation, boolean configurationComplete) {
        transaction(session -> {
            requireReady(session); new MongoCacheBusinessIdentity(database, business).requireValidationPreparation(session, operation);
            var row = schemas.find(session, new Document("_id", "business")).first();
            if (Integer.valueOf(2).equals(row.get("installedVersion"))) {
                var marker = requireValidationOperation(row, business, operation);
                if (configurationComplete && READY.equals(marker.get("phase"))) return null;
            } else {
                MongoValidationSchema.requireAbsent(database, MongoValidationSchema.CACHE);
                if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id.clusterId", business.clusterId())
                        .append("_id.resourceType", io.tapstate.spi.store.WorkloadClaimType.VALIDATION.name()))
                        .projection(new Document("_id", 0).append("found", new Document("$literal", true))).limit(1).first() != null) throw refused();
            }
            requireStoppedClaims(session, business); return null;
        });
    }

    private void requireStoppedClaims(ClientSession session, io.tapstate.spi.store.CacheBusinessScope business) {
        var unsafe = new Document("_id.clusterId", business.clusterId()).append("$or", java.util.List.of(
                new Document("leaseUntil", new Document("$not", new Document("$type", "date"))),
                new Document("$expr", new Document("$gt", java.util.List.of("$leaseUntil", "$$NOW")))));
        if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, unsafe)
                .projection(new Document("_id", 0).append("unsafe", new Document("$literal", true))).limit(1).first() != null) throw refused();
    }

    /** Offline business-schema preparation retains the original authentication and CACHE identities. */
    void prepareValidationSchema(io.tapstate.spi.store.CacheBusinessScope business, String operation, Runnable active) {
        var currentScope = new io.tapstate.spi.store.CacheBusinessScope(expected.clusterId(), expected.bindingVersion(), expected.cacheIncarnation());
        if (!currentScope.equals(business)) throw refused();
        transaction(session -> {
            active.run(); requireReady(session);
            var row = schemas.find(session, new Document("_id", "business")).first();
            if (row != null && Integer.valueOf(1).equals(row.get("installedVersion")) && !row.containsKey("validation")) {
                new MongoCacheBusinessIdentity(database, business).requireAdmissionState(session);
                fence(session);
                requireStoppedClaims(session, business);
                MongoValidationSchema.requireAbsent(database, MongoValidationSchema.CACHE);
                if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id.clusterId", business.clusterId())
                        .append("_id.resourceType", io.tapstate.spi.store.WorkloadClaimType.VALIDATION.name())).limit(1).first() != null) throw refused();
                if (schemas.updateOne(session, row, new Document("$set", new Document("installedVersion", 2)
                        .append("validation", MongoValidationSchema.marker(operation, PREPARING, 0, business)))).getMatchedCount() != 1) throw refused();
            } else requireValidationOperation(row, business, operation);
            active.run(); return null;
        });
        StoreIo.run(() -> {
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    active.run(); requireReady(session);
                    var row = schemas.find(session, new Document("_id", "business")).first();
                    var marker = requireValidationOperation(row, business, operation);
                    if (READY.equals(marker.get("phase"))) return null;
                    fence(session);
                    MongoValidationSchema.requireAbsent(database, MongoValidationSchema.CACHE);
                    MongoValidationSchema.create(session, database, MongoValidationSchema.CACHE);
                    if (schemas.updateOne(session, new Document("_id", "business").append("installedVersion", 2).append("validation", marker),
                            new Document("$set", new Document("validation", MongoValidationSchema.marker(operation, READY, 0, business)))).getMatchedCount() != 1)
                        throw refused();
                    active.run(); return null;
                }, PREPARATION);
            }
        });
        MongoValidationSchema.requireReady(database, false);
    }

    private static Document requireValidationOperation(Document row, io.tapstate.spi.store.CacheBusinessScope scope, String operation) {
        if (row == null || !Integer.valueOf(2).equals(row.get("installedVersion")) || !(row.get("validation") instanceof Document value))
            throw MongoValidationSchema.unreadable("version");
        var marker = MongoValidationSchema.validateMarker(value);
        if (!operation.equals(marker.get("operationId")) || !scope.equals(MongoValidationSchema.scope(marker)) || marker.getLong("initializedPipelines") != 0)
            throw refused();
        return marker;
    }
    <T> T transaction(Function<ClientSession, T> operation) {
        return StoreIo.call(() -> rawTransaction(operation));
    }

    <T> T transaction(MongoReadBudget budget, Function<ClientSession, T> operation) {
        return budget.transaction(client, operation);
    }

    private <T> T rawTransaction(Function<ClientSession, T> operation) {
        try (var session = client.startSession()) {
            return session.withTransaction(() -> operation.apply(session), TRANSACTION);
        }
    }

    void requireReady(ClientSession session) {
        Document row = requireIdentity(session);
        if (!READY.equals(row.get("authPhase"))) throw fenced();
        requireSchema(session, READY, AUTH_VERSION, row.getString("authPreparationId"));
    }

    /** Every security-changing request competes with generation publication on this same cache document. */
    void fence(ClientSession session) {
        Document row = requireIdentity(session);
        if (!READY.equals(row.get("authPhase"))) throw fenced();
        requireSchema(session, READY, AUTH_VERSION, row.getString("authPreparationId"));
        long counter = row.getLong("authWriteFence");
        if (identity.updateOne(session, scopeFilter(expected).append("authPhase", READY).append("authWriteFence", counter),
                new Document("$inc", new Document("authWriteFence", 1L))).getMatchedCount() != 1) {
            throw new IllegalStateException("cache authentication identity changed within its transaction snapshot");
        }
    }

    /** Only an explicitly authorized first preparation may create missing facts; ordinary loads never call this. */
    void beginPreparation(String operationId, boolean emptyTarget, Runnable requireActive) {
        beginPreparation(operationId, emptyTarget, requireActive, false);
    }

    void beginFirstPreparation(String operationId, boolean emptyTarget, Runnable requireActive) {
        beginPreparation(operationId, emptyTarget, requireActive, true);
    }

    private void beginPreparation(String operationId, boolean emptyTarget, Runnable requireActive, boolean businessBirth) {
        requireText(operationId);
        StoreIo.run(() -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    rawTransaction(session -> {
                        requireActive.run();
                        Document row = identity.find(session, new Document("_id", "cluster")).first();
                        if (row != null) {
                            row = requireIdentity(session);
                            if (!operationId.equals(row.get("authPreparationId"))) throw refused();
                            String phase = row.getString("authPhase");
                            requireSchema(session, phase, READY.equals(phase) ? AUTH_VERSION : 0, operationId);
                            if (businessBirth && (!PREPARING.equals(row.get("businessPhase")) || !operationId.equals(row.get("businessPreparationId")))) throw refused();
                        } else {
                            if (!emptyTarget || schemas.find(session).limit(1).first() != null) throw refused();
                            Document initial = scopeFilter(expected).append("layoutId", MongoConfigurationDomain.LAYOUT)
                                    .append("authPhase", PREPARING).append("authPreparationId", operationId)
                                    .append("authWriteFence", 0L).append("businessPhase", businessBirth ? PREPARING : "ABSENT");
                            if (businessBirth) initial.append("businessPreparationId", operationId);
                            identity.insertOne(session, initial);
                            schemas.insertOne(session, new Document("_id", "auth").append("installedVersion", 0)
                                    .append("epoch", 1L).append("operationId", operationId).append("phase", PREPARING));
                        }
                        requireActive.run();
                        return null;
                    });
                    return;
                } catch (MongoException concurrent) {
                    if (attempt >= 2 || ErrorCategory.fromErrorCode(concurrent.getCode()) != ErrorCategory.DUPLICATE_KEY) throw concurrent;
                }
            }
        });
    }

    /** Empty authentication structures and their readiness commit together in the original cache transaction. */
    void prepareStructure(String operationId, Runnable requireActive, Consumer<ClientSession> prepare) {
        StoreIo.run(() -> {
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    requireActive.run();
                    Document row = requireIdentity(session);
                    if (!operationId.equals(row.get("authPreparationId"))) throw refused();
                    if (READY.equals(row.get("authPhase"))) {
                        requireSchema(session, READY, AUTH_VERSION, operationId);
                        requireActive.run();
                        return null;
                    }
                    requireSchema(session, PREPARING, 0, operationId);
                    if (identity.updateOne(session, scopeFilter(expected).append("authPhase", PREPARING)
                                    .append("authPreparationId", operationId).append("authWriteFence", row.getLong("authWriteFence")),
                            new Document("$inc", new Document("authWriteFence", 1L))).getMatchedCount() != 1) throw refused();
                    prepare.accept(session);
                    if (identity.updateOne(session, scopeFilter(expected).append("authPhase", PREPARING)
                                    .append("authPreparationId", operationId), new Document("$set", new Document("authPhase", READY)))
                            .getMatchedCount() != 1) throw refused();
                    if (schemas.updateOne(session, new Document("_id", "auth").append("installedVersion", 0).append("epoch", 1L)
                                    .append("operationId", operationId).append("phase", PREPARING),
                            new Document("$set", new Document("installedVersion", AUTH_VERSION).append("phase", READY)))
                            .getMatchedCount() != 1) throw refused();
                    requireActive.run();
                    return null;
                }, PREPARATION);
            }
        });
    }

    /** Publish an independently authorized authentication reset while preserving the cache business incarnation. */
    CacheAuthScope advanceAuth(long nextGeneration, String operationId, Runnable requireActive) {
        requireText(operationId);
        if (nextGeneration != expected.authGeneration() + 1 || nextGeneration == Long.MAX_VALUE) throw refused();
        var next = new CacheAuthScope(expected.clusterId(), expected.bindingVersion(), expected.mode(), expected.cacheIncarnation(), nextGeneration);
        return transaction(session -> {
            requireActive.run();
            Document stored = identity.find(session, new Document("_id", "cluster")).first();
            if (matches(stored, next)) {
                new MongoCacheIdentity(client, identityDatabase(), next).requireReady(session);
                if (!operationId.equals(stored.get("authPreparationId"))) throw refused();
                requireActive.run();
                return next;
            }
            Document original = requireIdentity(session);
            if (!READY.equals(original.get("authPhase"))) throw fenced();
            requireSchema(session, READY, AUTH_VERSION, original.getString("authPreparationId"));
            if (identity.updateOne(session, scopeFilter(expected).append("authPhase", READY)
                            .append("authWriteFence", original.getLong("authWriteFence")),
                    new Document("$set", new Document("authGeneration", nextGeneration).append("authPreparationId", operationId))
                            .append("$inc", new Document("authWriteFence", 1L))).getMatchedCount() != 1) throw refused();
            if (schemas.updateOne(session, new Document("_id", "auth").append("installedVersion", AUTH_VERSION).append("phase", READY)
                            .append("operationId", original.get("authPreparationId")).append("epoch", 1L),
                    new Document("$set", new Document("operationId", operationId))).getMatchedCount() != 1) throw refused();
            requireActive.run();
            return next;
        });
    }

    private MongoDatabase identityDatabase() {
        return client.getDatabase(identity.getNamespace().getDatabaseName());
    }

    /** A verified CONFIG recovery intent competes on an existing formal authentication identity. */
    void beginConfirmedBusiness(String operationId, Runnable active, Runnable requireOnlyAuth) {
        transaction(session -> {
            active.run(); fence(session);
            Document row = requireIdentity(session);
            if (PREPARING.equals(row.get("businessPhase")) || READY.equals(row.get("businessPhase"))) {
                if (!operationId.equals(row.get("businessPreparationId"))) throw refused();
            } else {
                if (!"ABSENT".equals(row.get("businessPhase")) || row.containsKey("businessPreparationId")
                        || row.containsKey("claimAdmissionFence") || row.containsKey("businessRetirementId")) throw refused();
                requireOnlyAuth.run();
                if (schemas.find(session, new Document("_id", "business")).first() != null) throw refused();
                if (identity.updateOne(session, scopeFilter(expected).append("businessPhase", "ABSENT")
                                .append("businessPreparationId", new Document("$exists", false)),
                        new Document("$set", new Document("businessPhase", PREPARING).append("businessPreparationId", operationId)))
                        .getMatchedCount() != 1) throw refused();
            }
            active.run(); return null;
        });
    }

    private Document requireIdentity(ClientSession session) {
        Document row = identity.find(session, new Document("_id", "cluster")).first();
        if (row == null || !matches(row, expected)) throw fenced();
        if (!MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))
                || !(row.get("authWriteFence") instanceof Long counter) || counter < 0 || counter == Long.MAX_VALUE
                || !(row.get("authPreparationId") instanceof String operation) || operation.isBlank()
                || !(row.get("authPhase") instanceof String phase) || !Set.of(PREPARING, READY).contains(phase)
                || !(row.get("businessPhase") instanceof String business) || !Set.of("ABSENT", PREPARING, READY, "RETIRED").contains(business)) {
            throw unreadable();
        }
        return row;
    }
    private void requireSchema(ClientSession session, String phase, int installed, String operationId) {
        Document row = schemas.find(session, new Document("_id", "auth")).first();
        if (row == null || !(row.get("installedVersion") instanceof Integer version)) throw unreadable();
        if (version > AUTH_VERSION) throw new TapstateException(MigrationError.DATA_NEWER_THAN_BINARY,
                Map.of("installed", String.valueOf(version), "supported", String.valueOf(AUTH_VERSION)), null);
        if (version != installed || !phase.equals(row.get("phase")) || !Long.valueOf(1).equals(row.get("epoch"))
                || !operationId.equals(row.get("operationId"))) throw unreadable();
    }
    private static boolean matches(Document row, CacheAuthScope scope) {
        return row != null && scope.clusterId().equals(row.get("clusterId")) && Long.valueOf(scope.bindingVersion()).equals(row.get("bindingVersion"))
                && scope.mode().name().equals(row.get("mode")) && scope.cacheIncarnation().equals(row.get("cacheIncarnation"))
                && Long.valueOf(scope.authGeneration()).equals(row.get("authGeneration"));
    }
    private static Document scopeFilter(CacheAuthScope scope) {
        return new Document("_id", "cluster").append("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                .append("mode", scope.mode().name()).append("cacheIncarnation", scope.cacheIncarnation()).append("authGeneration", scope.authGeneration());
    }
    private static void requireText(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("cache preparation operation must not be blank");
    }
    static TapstateException fenced() { return new TapstateException(IoError.CACHE_AUTH_FENCED, Map.of(), null); }
    private static TapstateException refused() { return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null); }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-auth", "field", "identity"), null);
    }
}
