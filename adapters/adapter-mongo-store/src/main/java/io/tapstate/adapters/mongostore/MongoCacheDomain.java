package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SessionStore;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Owns the CACHE clients and keeps authentication preparation independent from business-domain assembly. */
public final class MongoCacheDomain implements io.tapstate.spi.store.PipelineStoreViews, io.tapstate.spi.store.SrsStoreViews, AutoCloseable {
    private static final String ORIGINAL_ARGUMENT = "original";
    private final MongoConnection connection;
    private final com.mongodb.client.MongoClient client;
    private final com.mongodb.client.MongoDatabase database;
    private final MongoClaimOperations claimOperations;
    private final MongoStructureProof structures = new MongoStructureProof();
    private final java.util.concurrent.atomic.AtomicReference<Set<SystemCollections>> inspectedCollections =
            new java.util.concurrent.atomic.AtomicReference<>(Set.of());
    private final MongoSrsControlProofs srsControlProofs = new MongoSrsControlProofs();
    private final Duration rateRetention;
    private final io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases;
    private final String targetFingerprint;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    private boolean connectionClosed;
    private com.mongodb.client.MongoClient nodeSessionClient;

    private MongoCacheDomain(MongoConnection connection, Duration rateRetention, String targetFingerprint,
                             io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases) {
        this.operatorDatabases = operatorDatabases;
        this.connection = connection; this.client = connection.client(); this.database = connection.database(); this.rateRetention = rateRetention; this.targetFingerprint = targetFingerprint;
        this.claimOperations = new MongoClaimOperations(client,true);
    }

    /** Connectivity alone creates no identity, schema, session, membership or business resource. */
    public static MongoCacheDomain connect(StorageBinding.Connection cache, Duration timeout, String processId) {
        return connect(cache, timeout, processId, MongoRateHistoryStore.DEFAULT_RETENTION);
    }

    public static MongoCacheDomain connect(StorageBinding.Connection cache, Duration timeout, String processId, Duration rateRetention) {
        return connect(cache, timeout, processId, rateRetention, null);
    }

    /** Extra operator databases require names captured by trusted CONFIG assembly, fixed for this owner. */
    public static MongoCacheDomain connect(StorageBinding.Connection cache, Duration timeout, String processId, Duration rateRetention,
                                          io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases) {
        rateRetention = MongoCacheMetadataStructure.requireRetention(rateRetention);
        if (operatorDatabases != null && !cache.database().equals(operatorDatabases.cacheDatabase())) throw MongoCachePipelineAccess.fenced();
        MongoConnection connection = MongoConnection.forCache(cache, timeout, processId);
        try {
            connection.verifyConnectivity();
            return new MongoCacheDomain(connection, rateRetention, MongoStorageBindingStore.targetFingerprint(cache), operatorDatabases);
        } catch (RuntimeException | Error failure) {
            connection.close(); throw failure;
        }
    }

    /** Reads the original authentication identity and its prepared schema marker. */
    public CacheAuthScope loadAuthScope(CacheAuthScope expected) {
        var facts = facts(expected);
        facts.transaction(session -> { facts.requireReady(session); return null; });
        return expected;
    }

    /** Request-time validation keeps the original generation and bounds every dependency read. */
    public CacheAuthScope loadAuthScope(CacheAuthScope expected, Duration timeout) {
        return loadAuthScope(expected, new MongoReadBudget(timeout));
    }

    public CacheAuthScope loadAuthScope(CacheAuthScope expected, MongoReadBudget budget) {
        Objects.requireNonNull(budget, "budget");
        var facts = facts(expected);
        facts.transaction(budget, session -> { facts.requireReady(session); return null; });
        budget.requireRemaining();
        return expected;
    }
    /** Request admission reads the original authentication generation once. */
    public CacheAuthScope loadRequestAuthScope(CacheAuthScope expected, MongoReadBudget budget) {
        var facts = facts(expected);
        facts.requireRequestReady(budget);
        return expected;
    }

    void requireRecoveryTarget(StorageBinding binding) {
        businessActive();
        if (!targetFingerprint.equals(MongoStorageBindingStore.targetFingerprint(binding.cache()))) throw MongoCacheIdentity.fenced();
    }
    boolean lacksSnapshotRead() { return MongoValidationClaimObservation.lacksSnapshotRead(client); }

    java.util.Optional<io.tapstate.spi.store.WorkloadClaimReading> observeValidationClaim(
            MongoValidationClaimObservation observation, MongoValidationPreparationFacts.Subject subject) {
        businessActive();
        var result = observation.cache(database, subject);
        businessActive();
        return result;
    }
    java.util.Optional<MongoConfigurationDomain.ValidationAccess.ClosedCountLedger> observeClosedCountLedger(
            MongoValidationClaimObservation observation, MongoValidationPreparationFacts.Subject subject,
            io.tapstate.core.validation.ValidationOwner current) {
        businessActive();
        if (lacksSnapshotRead()) return java.util.Optional.empty();
        var result = observation.closedCountLedger(database, subject, current);
        businessActive();
        return result;
    }
    boolean emptyForAuthenticationRecovery() {
        businessActive();
        return StoreIo.call(() -> database.listCollectionNames().into(new ArrayList<>()).isEmpty());
    }
    void prepareRecoveryAuthentication(io.tapstate.spi.store.CachePreparation original, Runnable pending) {
        pending.run(); requireAuthOnlyRecovery(original.authScope());
        connection.verifyStoragePermissions();
        prepareAuth(original.authScope(), original.operationId(), pending);
        requireAuthOnlyRecovery(original.authScope());
        var current = facts(original.authScope());
        current.transaction(session -> { pending.run(); current.fence(session); pending.run(); return null; });
        loadAuthScope(original.authScope()); pending.run();
    }
    void verifyRecoveryPermissions() { businessActive(); connection.verifyStoragePermissions(); }
    /** Initial OP preflight has no formal session and must observe a genuinely empty target. */
    public void verifyEmptyInitialRecovery() {
        businessActive(); if (!emptyForAuthenticationRecovery()) throw MongoCacheIdentity.fenced();
    }
    public void verifyInitialRecoveryPermissions() { verifyRecoveryPermissions(); }

    void verifyInitialRecovery(io.tapstate.spi.store.CachePreparation original) {
        businessActive(); loadAuthScope(original.authScope()); loadBusinessScope(original.businessScope());
        StoreIo.run(() -> {
            var row = SystemCollections.CACHE_IDENTITY.on(database).find(new Document("_id", "cluster")).first();
            if (row == null || !original.operationId().equals(row.get("authPreparationId"))
                    || !original.operationId().equals(row.get("businessPreparationId")) || !"READY".equals(row.get("businessPhase"))) throw MongoCacheIdentity.fenced();
        });
    }
    /** A new preflight must observe exactly the current formal auth-only domain. */
    public void verifyAuthenticationOnly(CacheAuthScope expected) {
        businessActive(); loadAuthScope(expected); requireAuthOnlyRecovery(expected);
    }

    void prepareConfirmedInitialBusiness(io.tapstate.spi.store.CachePreparation original, Runnable active) {
        active.run(); loadAuthScope(original.authScope());
        facts(original.authScope()).beginConfirmedBusiness(original.operationId(), active, () -> requireAuthOnlyRecovery(original.authScope()));
        prepareInitialBusiness(original, active);
    }
    private void requireAuthOnlyRecovery(CacheAuthScope expected) {
        StoreIo.run(() -> {
            var allowed = new java.util.HashSet<String>();
            allowed.add(SystemCollections.CACHE_IDENTITY.collectionName()); allowed.add(SystemCollections.CACHE_SCHEMA_MIGRATIONS.collectionName());
            authTables(expected.mode()).forEach(table -> allowed.add(table.collectionName()));
            for (String name : database.listCollectionNames()) if (!allowed.contains(name)) throw MongoCacheIdentity.fenced();
            var identities = SystemCollections.CACHE_IDENTITY.on(database).find().limit(2).into(new ArrayList<>());
            if (identities.size() > 1 || identities.size() == 1 && (!"cluster".equals(identities.getFirst().get("_id"))
                    || !"ABSENT".equals(identities.getFirst().get("businessPhase")))) throw MongoCacheIdentity.fenced();
            var schemas = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find().limit(2).into(new ArrayList<>());
            if (schemas.size() > 1 || schemas.size() == 1 && !"auth".equals(schemas.getFirst().get("_id"))) throw MongoCacheIdentity.fenced();
        });
    }

    /** The scope was durably allocated by the original configuration setup or recovery operation. */
    public CacheAuthScope prepareAuth(CacheAuthScope scope, String operationId, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        requireActive.run();
        boolean emptyTarget = StoreIo.call(() -> {
            try (var names = connection.database().listCollectionNames().iterator()) { return !names.hasNext(); }
        });
        var facts = facts(scope);
        facts.beginPreparation(operationId, emptyTarget, requireActive);
        return finishAuth(facts, scope, operationId, requireActive);
    }

    /** Only the confirmed CONFIG first winner may create the original business birth marker. */
    public CacheAuthScope prepareFirstAuth(io.tapstate.spi.store.FirstCacheCreation invocation) {
        var original = MongoStorageBindingStore.consumeFirst(invocation, targetFingerprint);
        var preparation = original.preparation();
        Runnable active = () -> { businessActive(); MongoStorageBindingStore.requireFirstLocal(original); };
        original.requireActive(); active.run();
        boolean emptyTarget = StoreIo.call(() -> { try (var names = database.listCollectionNames().iterator()) { return !names.hasNext(); } });
        var facts = facts(preparation.authScope());
        facts.beginFirstPreparation(preparation.operationId(), emptyTarget, active);
        original.requireActive();
        var result = finishAuth(facts, preparation.authScope(), preparation.operationId(), active);
        original.requireActive(); active.run(); return result;
    }

    /** A controlled continuation may complete retained facts but never recreate a missing identity. */
    public CacheAuthScope resumeInitialAuth(io.tapstate.spi.store.CachePreparation original, Runnable requireActive) {
        Objects.requireNonNull(original, ORIGINAL_ARGUMENT); Objects.requireNonNull(requireActive, "requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run(); var facts = facts(original.authScope());
        facts.beginPreparation(original.operationId(), false, active);
        return finishAuth(facts, original.authScope(), original.operationId(), active);
    }

    private CacheAuthScope finishAuth(MongoCacheIdentity facts, CacheAuthScope scope, String operationId, Runnable requireActive) {
        facts.prepareStructure(operationId, requireActive, session -> {
            for (var table : authTables(scope.mode())) {
                requireActive.run();
                table.prepareCollectionOn(connection.database(), session);
                for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(connection.database()), index);
            }
        });
        verifyAuthCollections(scope.mode());
        return loadAuthScope(scope);
    }

    /** An authorized reset publishes one next auth generation on the same cache business incarnation. */
    public CacheAuthScope advanceAuthGeneration(CacheAuthScope original, long nextGeneration, String operationId, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        requireActive.run();
        verifyAuthCollections(original.mode());
        var next = facts(original).advanceAuth(nextGeneration, operationId, requireActive);
        return loadAuthScope(next);
    }

    /** An OP session view captures this exact scope and shares the cache client's lifecycle. */
    public SessionStore sessions(CacheAuthScope scope) {
        if (scope.mode() != StorageBinding.Mode.OP) throw MongoCacheIdentity.fenced();
        loadAuthScope(scope);
        return new MongoCacheSessionStore(facts(scope), connection.database());
    }

    /** A managed-session view does not adopt a later authentication generation. */
    public CloudSessionStore cloudSessions(CacheAuthScope scope) {
        if (scope.mode() != StorageBinding.Mode.CLOUD) throw MongoCacheIdentity.fenced();
        loadAuthScope(scope);
        return new MongoCacheCloudSessionStore(facts(scope), connection.database(), null);
    }

    /** A request-owned view retains its scope and deadline; each operation verifies its original scope and fence. */
    public CloudSessionStore cloudSessions(CacheAuthScope scope, MongoReadBudget budget) {
        if (scope.mode() != StorageBinding.Mode.CLOUD) throw MongoCacheIdentity.fenced();
        Objects.requireNonNull(budget, "budget").requireRemaining();
        // This view is owned by an admitted HTTP operation. Installation and recovery validate structures;
        // its transaction still reads current identity, schema, revocation and session facts.
        return new MongoCacheCloudSessionStore(facts(scope), connection.database(), budget);
    }

    /** Verifies a fixed business identity and its schema; individual facets verify their own structures. */
    public io.tapstate.spi.store.CaptureCachePreparations capturePreparations(CacheBusinessScope scope,Runnable requireActive) {
        Objects.requireNonNull(requireActive,"requireActive");return new MongoCaptureCachePreparations(client,database,scope,targetFingerprint,() -> {businessActive();requireActive.run();},rateRetention);
    }
    public io.tapstate.spi.store.SharedSrsCacheRecoveries sharedSrsRecoveries(CacheBusinessScope scope,Runnable requireActive) {
        Objects.requireNonNull(requireActive,"requireActive");return new MongoCaptureCachePreparations(client,database,scope,targetFingerprint,
                () -> {businessActive();requireActive.run();},rateRetention);
    }

    public CacheBusinessScope loadBusinessScope(CacheBusinessScope expected) {
        businessActive();
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    businessActive();
                    new MongoCacheBusinessIdentity(database, expected).requireAdmissionState(session);
                    businessActive();
                    return expected;
                }, businessTransaction());
            } catch (RuntimeException failure) { businessActive(); throw failure; }
        });
    }

    /** Catalog-only inspection for the active business owner. Optional features use their preparation markers. */
    public List<MongoStructureInspection.Difference> inspectStructure(CacheAuthScope auth, MongoReadBudget budget) {
        businessActive();
        final long inspectionGeneration;
        synchronized (structures) {
            inspectedCollections.set(Set.of());
            structures.clear();
            inspectionGeneration = structures.currentGeneration();
        }
        var required = java.util.EnumSet.copyOf(MongoCacheMetadataStructure.BUSINESS_TABLES);
        required.add(SystemCollections.CACHE_IDENTITY);
        required.add(SystemCollections.CACHE_SCHEMA_MIGRATIONS);
        if (auth.mode() == StorageBinding.Mode.OP) {
            required.add(SystemCollections.OP_SESSIONS);
        } else {
            required.add(SystemCollections.CLOUD_SESSIONS);
            required.add(SystemCollections.CLOUD_REVOCATIONS);
        }
        var business = StoreIo.call(() -> SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(budget.database(database))
                .find(new Document("_id", "business")).first());
        if (business != null && business.get("validation") instanceof Document validation
                && "READY".equals(validation.getString("phase"))) required.addAll(MongoValidationSchema.CACHE);
        var result = new ArrayList<>(MongoStructureInspection.inspect(database,
                MongoStructureInspection.namespaces(required, required, rateRetention), budget));
        var scope = new CacheBusinessScope(auth.clusterId(), auth.bindingVersion(), auth.cacheIncarnation());
        if (operatorDatabases != null && operatorDatabases.scope().equals(scope)) {
            var namespaces = MongoStructureInspection.namespaces(MongoCacheOperatorDatabases.TABLES,
                    MongoCacheOperatorDatabases.TABLES, rateRetention);
            for (var name : budget.transaction(client, session -> MongoOperatorInventory.registeredDatabases(session, database, scope))) {
                MongoStorePort.requireOperatorStateDatabase(name, database.getName());
                MongoStorePort.requireOperatorStateDatabase(name, operatorDatabases.configurationDatabase());
                MongoStorePort.requireOperatorStateDatabase(name, operatorDatabases.masterDatabase());
                result.addAll(MongoStructureInspection.inspect(client.getDatabase(name), namespaces, budget));
            }
        }
        businessActive();
        var eligible = java.util.EnumSet.copyOf(required);
        eligible.removeIf(table -> table.kind() != SystemCollections.Kind.COLLECTION
                || result.stream().anyMatch(difference -> database.getName().equals(difference.database())
                        && table.physicalNames().contains(difference.collection())));
        synchronized (structures) {
            if (structures.currentGeneration() == inspectionGeneration) inspectedCollections.set(Set.copyOf(eligible));
        }
        return List.copyOf(result);
    }

    /** Prepares the newly introduced aggregate structure before admitting a retained business scope. */
    public void prepareRetainedAggregateState(CacheBusinessScope expected, Runnable requireActive) {
        Runnable active = () -> { businessActive(); requireActive.run(); };
        var structure = new MongoCacheMetadataStructure(database, rateRetention);
        StoreIo.run(() -> {
            active.run();
            if (structure.hasAggregateState()) return;
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    active.run();
                    new MongoCacheBusinessIdentity(database, expected).admitMetadataWrite(session);
                    structure.prepareRetainedAggregateStateInTransaction(session, expected, active);
                    active.run();
                    return null;
                }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.LOCAL)
                        .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build());
            }
            active.run();
        });
    }

    /** Admission validates retained business structures and configured indexes without repairing them. */
    public void verifyBusinessReady(CacheBusinessScope expected) {
        loadBusinessScope(expected);
        new MongoCacheMetadataStructure(database, rateRetention).verifyBusiness();
        loadBusinessScope(expected);
    }

    /** Controlled continuation requires the original operation and already retained authentication facts. */
    public CacheBusinessScope prepareInitialBusiness(io.tapstate.spi.store.CachePreparation original, Runnable requireActive) {
        Objects.requireNonNull(original, ORIGINAL_ARGUMENT); Objects.requireNonNull(requireActive, "requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run();
        return new MongoCacheBusinessPreparation(client, database, original, rateRetention, active).prepare();
    }

    MongoValidationCachePreparations validationPreparations(CacheBusinessScope expected) {
        return validationPreparations(expected, () -> {});
    }
    MongoValidationCachePreparations validationPreparations(CacheBusinessScope expected, Runnable local) {
        loadBusinessScope(expected);
        StoreIo.run(() -> MongoValidationSchema.requireReady(database, false));
        return new MongoValidationCachePreparations(client, database, expected, targetFingerprint, () -> { businessActive(); local.run(); });
    }

    WorkloadClaimStore validationClaims(MongoValidationPreparations.Permission original) {
        return validationClaims(new MongoValidationPreparations.Authority(original));
    }
    WorkloadClaimStore validationClaims(MongoValidationPreparations.Permission original, Runnable local) {
        return validationClaims(new MongoValidationPreparations.Authority(original), local);
    }

    io.tapstate.spi.store.ValidationControlStore validationControl(MongoValidationPreparations.Permission original) {
        return validationControl(new MongoValidationPreparations.Authority(original));
    }
    io.tapstate.spi.store.ValidationControlStore validationControl(MongoValidationPreparations.Permission original, Runnable local) {
        return validationControl(original, null, local);
    }
    io.tapstate.spi.store.ValidationControlStore validationControl(MongoValidationPreparations.Permission original,
            io.tapstate.core.validation.ValidationOwner countOwner, Runnable local) {
        var authority = new MongoValidationPreparations.Authority(original, countOwner);
        requireValidationPreparation(authority);
        return validationControlHandle(authority, local);
    }

    io.tapstate.spi.store.ValidationControlStore validationControlHandle(MongoValidationPreparations.Authority authority, Runnable local) {
        return new MongoValidationControlStore(client, database, authority.subject().scope(),
                () -> { businessActive(); local.run(); }, authority);
    }

    io.tapstate.spi.store.ValidationRunHistory validationHistory(MongoValidationPreparations.Permission original,Runnable local){
        var authority=new MongoValidationPreparations.Authority(original);requireValidationPreparation(authority);
        return new MongoValidationControlStore(client,database,authority.subject().scope(),()->{businessActive();local.run();},authority);
    }

    WorkloadClaimStore validationClaims(MongoValidationPreparations.Closing original) {
        return validationClaims(new MongoValidationPreparations.Authority(original));
    }

    io.tapstate.spi.store.ValidationControlStore validationControl(MongoValidationPreparations.Closing original) {
        return validationControl(new MongoValidationPreparations.Authority(original));
    }
    io.tapstate.spi.store.ValidationControlStore validationControl(MongoValidationPreparations.Closing original, Runnable local) {
        var authority = new MongoValidationPreparations.Authority(original); requireValidationPreparation(authority);
        return new MongoValidationControlStore(client, database, authority.subject().scope(), () -> { businessActive(); local.run(); }, authority);
    }

    private WorkloadClaimStore validationClaims(MongoValidationPreparations.Authority original) {
        return validationClaims(original, () -> {});
    }
    MongoCacheWorkloadClaimStore validationCountClaims(MongoValidationPreparations.Permission original, Runnable local) {
        businessActive(); local.run();
        // ValidationAccess already holds the exact verified binding. Claim operations recheck their durable fences.
        return validationClaimHandle(new MongoValidationPreparations.Authority(original), local);
    }
    private MongoCacheWorkloadClaimStore validationClaims(MongoValidationPreparations.Authority original, Runnable local) {
        Objects.requireNonNull(local);
        requireValidationPreparation(original);
        return validationClaimHandle(original, local);
    }
    private MongoCacheWorkloadClaimStore validationClaimHandle(MongoValidationPreparations.Authority original, Runnable local) {
        return new MongoCacheWorkloadClaimStore(client, database, original.subject().scope(), () -> { businessActive(); local.run(); }, original);
    }

    private io.tapstate.spi.store.ValidationControlStore validationControl(MongoValidationPreparations.Authority original) {
        requireValidationPreparation(original);
        return new MongoValidationControlStore(client, database, original.subject().scope(), this::businessActive, original);
    }

    private void requireValidationPreparation(MongoValidationPreparations.Authority original) {
        var binding = original.binding();
        if (!targetFingerprint.equals(MongoStorageBindingStore.targetFingerprint(binding.cache()))) throw MongoCacheIdentity.fenced();
        loadBusinessScope(original.subject().scope());
        StoreIo.run(() -> MongoValidationSchema.requireReady(database, false));
    }

    void prepareValidationSchema(io.tapstate.spi.store.CachePreparation original, String operation) {
        businessActive();
        facts(original.authScope()).prepareValidationSchema(original.businessScope(), operation, this::businessActive);
    }

    /** Explicit original recovery only; ordinary business admission still refuses partial schemas. */
    void resumeValidationSchema(io.tapstate.spi.store.CachePreparation original, String operation) {
        businessActive();
        var schema = StoreIo.call(() -> SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find(new Document("_id", "business")).first());
        if (schema != null && Integer.valueOf(2).equals(schema.get("installedVersion"))
                && schema.get("validation") instanceof Document marker && "PREPARING".equals(marker.get("phase")))
            prepareValidationSchema(original, operation);
    }

    void verifyValidationMaintenance(io.tapstate.spi.store.CachePreparation original, String operation, boolean configurationComplete) {
        businessActive();
        new MongoCacheMetadataStructure(database, rateRetention).verifyBusiness();
        facts(original.authScope()).verifyValidationMaintenance(original.businessScope(), operation, configurationComplete);
    }

    void verifyStoppedValidationMaintenance(io.tapstate.spi.store.CachePreparation original, String originalOperation) {
        businessActive(); new MongoCacheMetadataStructure(database, rateRetention).verifyBusiness();
        facts(original.authScope()).verifyStoppedValidationMaintenance(original.businessScope(), originalOperation);
    }

    void verifyValidationReady(io.tapstate.spi.store.CachePreparation original, String operation) {
        verifyValidationMaintenance(original, operation, true);
        StoreIo.run(() -> {
            MongoValidationSchema.requireReady(database, false);
            MongoValidationSchema.verify(database, MongoValidationSchema.CACHE);
        });
        businessActive();
    }

    /** All claims share the original CACHE scope and its lifetime. */
    public WorkloadClaimStore workloadClaims(CacheBusinessScope expected) {
        claimOperations.requireAdmission();
        loadBusinessScope(expected);
        MongoCacheAdmissionQueue.bind(client, database.getName(), expected, claimOperations);
        return new MongoCacheWorkloadClaimStore(client, database, expected, this::businessActive, null,
                claimOperations, this::nodeSessionClient);
    }

    private synchronized com.mongodb.client.MongoClient nodeSessionClient() {
        businessActive();
        if (nodeSessionClient == null) nodeSessionClient = connection.openNodeSessionClient();
        return nodeSessionClient;
    }

    public io.tapstate.spi.store.ClusterMembershipStore clusterMembership(CacheBusinessScope expected) {
        return new MongoCacheClusterMembershipStore(client, database, expected, null, this::businessActive);
    }
    public io.tapstate.spi.store.ClusterMembershipStore clusterMembership(CacheBusinessScope expected,
            io.tapstate.spi.store.WorkloadClaimFence originalNode, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        return new MongoCacheClusterMembershipStore(client, database, expected, Objects.requireNonNull(originalNode, "originalNode"),
                () -> { businessActive(); requireActive.run(); });
    }

    /** Explicit position writes keep all selected native resources in the same CACHE transaction. */
    public io.tapstate.spi.store.SourcePositionEdits sourcePositions(CacheBusinessScope expected,
            io.tapstate.spi.store.WorkloadClaimFence node, Runnable requireActive) {
        Objects.requireNonNull(requireActive); Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run(); loadBusinessScope(expected);
        return new MongoSrsConsumerStore(client, database, expected, active, () -> {}).positions(node);
    }

    /** SRS facets retain one CACHE client, complete business scope and original caller lifecycle. */
    @Override public io.tapstate.spi.store.SrsStores srs(CacheBusinessScope expected, Runnable requireActive) {
        return srs(expected, requireActive, false);
    }

    /** Reads an original consumer cursor in its own CACHE admission snapshot. */
    public java.util.Optional<io.tapstate.spi.store.ConsumerOffset> observationCursor(CacheBusinessScope expected,
            io.tapstate.spi.store.SrsConsumerContext context, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        return new MongoSrsConsumerStore(client, database, expected, active, () -> {})
                .admittedObservationCursor(context);
    }

    /** One validated view for the synchronous reads of a start plan. */
    public io.tapstate.spi.store.SrsStores startPlanningSrs(CacheBusinessScope expected, Runnable requireActive) {
        return srs(expected, requireActive, false, false);
    }

    public io.tapstate.spi.store.SrsStores runtimeSrs(CacheBusinessScope expected, Runnable requireActive) {
        return srs(expected, requireActive, true);
    }

    private io.tapstate.spi.store.SrsStores srs(CacheBusinessScope expected, Runnable requireActive, boolean runtime) {
        return srs(expected, requireActive, runtime, true);
    }

    private io.tapstate.spi.store.SrsStores srs(CacheBusinessScope expected, Runnable requireActive, boolean runtime,
            boolean loadScope) {
        Objects.requireNonNull(requireActive, "requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run();
        if (loadScope) loadBusinessScope(expected);
        active.run();
        Runnable verifyStructure = () -> {};
        var lease = MongoSrsLease.selected();
        boolean batchRuntime = runtime && !"transaction".equals(System.getProperty(MongoSrsLease.VALIDATION_PROPERTY));
        java.util.function.BooleanSupplier relations = batchRuntime ? this::canBatchSrsControlSnapshot : () -> false;
        java.util.function.BooleanSupplier readiness = batchRuntime ? this::canBatchSrsControlReadiness : () -> false;
        var models = new MongoSrsModelStore(client, database, expected, active, verifyStructure, lease, relations, readiness,
                batchRuntime ? this::canBatchSrsReclaimReferences : () -> false);
        return new io.tapstate.spi.store.SrsStores(
                new MongoSrsChainStore(client, database, expected, active, verifyStructure, lease,srsControlProofs), models,
                new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models, lease),
                new MongoSrsConsumerStore(client, database, expected, active, verifyStructure, MongoSrsLease.selected(),
                        runtime, srsControlProofs, relations, readiness, claimOperations));
    }

    private boolean canBatchSrsControlSnapshot() {
        return MongoReadBudget.supportsSnapshotReads(client) && inspectedCollections.get().containsAll(List.of(
                SystemCollections.SRS_CHAINS, SystemCollections.SRS_CONSUMERS, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS));
    }

    private boolean canBatchSrsControlReadiness() {
        return inspectedCollections.get().containsAll(List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS));
    }

    private boolean canBatchSrsReclaimReferences() {
        return inspectedCollections.get().containsAll(List.of(SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS,
                SystemCollections.SRS_LOG_BOUNDS));
    }

    @Override public io.tapstate.spi.store.SnapshotStateStore snapshotStates(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original,Runnable requireActive) {
        if (Objects.requireNonNull(original,ORIGINAL_ARGUMENT).executionGeneration()<1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoSnapshotStateStore(pipelineAccess(scope,original,requireActive));
    }

    /** The private direct-channel view retains this exact CACHE identity, client and lifecycle. */
    @Override public io.tapstate.spi.store.DirectChannelStore directChannels(CacheBusinessScope expected,Runnable requireActive) {
        Objects.requireNonNull(requireActive,"requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run(); loadBusinessScope(expected);
        return new MongoDirectChannelStore(client,database,expected,active);
    }

    public io.tapstate.spi.store.SchemaStore schemas(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheSchemaStore(metadataAccess(expected, SystemCollections.CACHE_SOURCE_SCHEMAS, requireActive));
    }

    /** Start planning reads the original schema and business scope. */
    public io.tapstate.spi.store.SchemaStore admittedStartSchemas(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheSchemaStore(metadataAccess(expected, SystemCollections.CACHE_SOURCE_SCHEMAS, requireActive), true);
    }

    public io.tapstate.spi.store.ConnectorRegistry connectors(CacheBusinessScope expected, Runnable requireActive) {
        businessActive();
        var bucket = SystemCollections.CACHE_CONNECTOR_ARTIFACTS;
        var structure = new MongoCacheMetadataStructure(database, rateRetention);
        if (MongoCollectionMetadata.matching(database, java.util.Set.copyOf(bucket.physicalNames()), false).isEmpty()) {
            StoreIo.run(() -> {
                Runnable active = () -> { businessActive(); requireActive.run(); };
                active.run();
                try (var session = client.startSession()) {
                    session.withTransaction(() -> {
                        active.run();
                        new MongoCacheBusinessIdentity(database, expected).admitMetadataWrite(session);
                        structure.prepareLegacyConnectorArtifactsInTransaction(session, active);
                        return null;
                    }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.LOCAL)
                            .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build());
                }
            });
        }
        return new MongoCacheConnectorRegistry(client, database, expected, this::businessActive, requireActive);
    }

    public boolean copyLegacyConnectorArtifact(CacheBusinessScope scope, Runnable requireActive,
                                               String hash, java.io.OutputStream output) throws java.io.IOException {
        requireActive.run();
        var bucket = SystemCollections.CACHE_CONNECTOR_ARTIFACTS;
        var structures = MongoCollectionMetadata.matching(database, java.util.Set.copyOf(bucket.physicalNames()), false);
        if (structures.isEmpty()) return false;
        return new MongoCacheConnectorRegistry(client, database, scope, this::businessActive, requireActive)
                .copyArtifactTo(hash, output);
    }

    public io.tapstate.spi.store.ConnectorCatalogStore connectorCatalog(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectorCatalogStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTOR_CATALOG, requireActive));
    }
    public io.tapstate.spi.store.ConnectorArtifactHoldings connectorArtifactHoldings(CacheBusinessScope expected, Runnable requireActive) {
        Runnable active = () -> { businessActive(); requireActive.run(); };
        MongoCacheConnectorArtifactHoldings.prepare(client, database, expected, active);
        return new MongoCacheConnectorArtifactHoldings(metadataAccess(expected, SystemCollections.CACHE_CONNECTOR_ARTIFACT_HOLDINGS, requireActive));
    }
    public io.tapstate.spi.store.ConnectorSpecStore connectorSpecs(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectorSpecStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTOR_SPECS, requireActive));
    }
    public io.tapstate.spi.store.ConnectionTestResultStore connectionTestResults(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectionTestResultStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTION_TEST_RESULTS, requireActive));
    }
    /** Publishes bounded reports against the baseline captured at test admission. */
    public io.tapstate.spi.store.ConnectionTestResultStore diagnosticResults(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectionTestResultStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTION_TEST_RESULTS, requireActive), true);
    }
    private MongoCacheMetadataAccess metadataAccess(CacheBusinessScope expected, SystemCollections table, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        businessActive();
        return new MongoCacheMetadataAccess(client, database, expected, table,
                () -> { businessActive(); requireActive.run(); });
    }

    /** Controlled first preparation retains this exact cache domain and caller lifecycle. */
    public io.tapstate.spi.store.PipelineCachePreparations pipelinePreparations(CacheBusinessScope expected, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        MongoCacheAdmissionQueue.bind(client, database.getName(), expected, claimOperations);
        return new MongoCachePipelinePreparations(client, database, expected, () -> { businessActive(); requireActive.run(); });
    }

    /** The native member has already verified the business collections during storage admission. */
    public io.tapstate.spi.store.PipelineCachePreparations admittedPipelinePreparations(CacheBusinessScope expected, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        MongoCacheAdmissionQueue.bind(client, database.getName(), expected, claimOperations);
        return new MongoCachePipelinePreparations(client, database, expected, () -> { businessActive(); requireActive.run(); }, true);
    }

    public void requireCurrentPipelineRetirement(io.tapstate.spi.store.PipelineRetirementReceipt receipt,
            io.tapstate.spi.store.WorkloadClaimFence node) {
        businessActive();
        MongoCachePipelineRetirements.requireCurrentReceipt(receipt, Objects.requireNonNull(node));
    }

    public io.tapstate.spi.store.PipelineCacheRetirements pipelineRetirements(CacheBusinessScope expected,Runnable requireActive) {
        Objects.requireNonNull(requireActive);return new MongoCachePipelineRetirements(client,database,expected,() -> {businessActive();requireActive.run();});
    }

    /** Controlled successor preparation retains the original history, client and caller lifecycle. */
    public io.tapstate.spi.store.RetiredPipelinePayloads retiredPipelinePayloads(CacheBusinessScope expected,Runnable requireActive) {
        Objects.requireNonNull(requireActive);loadBusinessScope(expected);return new MongoRetiredPipelinePayloads(client,database,expected,() -> {businessActive();requireActive.run();},operatorDatabases);
    }

    public io.tapstate.spi.store.PipelineCacheRebuilds pipelineRebuilds(CacheBusinessScope expected, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        return new MongoCachePipelineRebuilds(client, database, expected, () -> { businessActive(); requireActive.run(); }, rateRetention, operatorDatabases);
    }

    @Override public io.tapstate.spi.store.SinkPreparationStore sinkPreparations(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original,Runnable requireActive) {
        return new MongoSinkPreparationStore(pipelineAccess(scope,original,requireActive));
    }

    /** Read-only inspection of a database explicitly named by the confirmed Pipeline definition. */
    public void inspectRecoveredOperatorDatabase(io.tapstate.spi.store.PipelineCacheRecovery summary,String targetDatabase,Runnable active) {
        Objects.requireNonNull(summary);Objects.requireNonNull(active);active.run();
        var access=pipelineAccess(summary.targetScope(),null,active);
        var target=access.operatorDatabase(targetDatabase,io.tapstate.spi.store.OperatorPlacement.Resource.NEST_STATE);
        if(target.getName().equals(database.getName()))return;
        access.transaction(session -> {MongoCacheOperatorDatabases.inspectRecovery(session,target,operatorDatabases,summary.previousScope());return null;});
        active.run();
    }
    /** Only a native confirmed full may rebind a surviving extra cache, after the replacement Pipeline has been prepared. */
    public void prepareRecoveredOperatorDatabase(io.tapstate.spi.store.CacheRecoveryStore.FullPreparation confirmation,
            io.tapstate.spi.store.WorkloadClaimFence node,String targetDatabase,Runnable active) {
        if(!(confirmation instanceof MongoCacheRecoveryStore.Permission permission))throw MongoCachePipelineAccess.fenced();
        Objects.requireNonNull(node);Objects.requireNonNull(active);active.run();permission.requireCacheTarget(this);
        var summary=permission.recovery();var access=pipelineAccess(summary.targetScope(),null,active);
        var target=access.operatorDatabase(targetDatabase,io.tapstate.spi.store.OperatorPlacement.Resource.NEST_STATE);
        if(!target.getName().equals(database.getName()))access.recoveredOperatorPreparation(node,session -> {
            var inventory=MongoOperatorInventory.require(session,database,summary.targetScope());
            String preparation=MongoCacheOperatorDatabases.recover(session,database,target,operatorDatabases,summary.previousScope(),permission.operationId(),active);
            MongoOperatorInventory.rememberDatabase(session,database,summary.targetScope(),inventory,target.getName(),preparation);return null;
        });
        active.run();permission.requireCacheTarget(this);
    }

    /** Explicit schema preparation is separate from every ordinary operator facet open. */
    @Override public void prepareOperatorDatabase(io.tapstate.spi.store.PipelineIntentScope scope, String targetDatabase,
            io.tapstate.spi.store.WorkloadClaimFence original, String operationId, Runnable requireActive) {
        if (Objects.requireNonNull(original, ORIGINAL_ARGUMENT).executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        MongoCachePipelineAccess access = pipelineAccess(scope, original, requireActive);
        var target = access.operatorDatabase(targetDatabase, io.tapstate.spi.store.OperatorPlacement.Resource.NEST_STATE);
        if (target.getName().equals(database.getName())) {
            access.metadataTransaction(SystemCollections.CACHE_NEST_STATE, true, session -> {
                var structure = new MongoCacheMetadataStructure(database, rateRetention);
                structure.verifyFacet(SystemCollections.CACHE_NEST_STATE);
                structure.verifyFacet(SystemCollections.CACHE_NEST_SHAPES);
                return null;
            });
            return;
        }
        access.operatorPreparation(session -> {
            var inventory = MongoOperatorInventory.require(session, database, scope);
            String stored = MongoCacheOperatorDatabases.prepare(session, database, target, operatorDatabases, operationId, requireActive);
            MongoOperatorInventory.rememberDatabase(session, database, scope, inventory, target.getName(), stored);
            return null;
        });
    }

    @Override public io.tapstate.spi.store.NestShapeStore nestShapes(io.tapstate.spi.store.PipelineIntentScope scope, String database) {
        return new MongoCacheNestShapeStore(pipelineAccess(scope, null, () -> {}), database);
    }

    @Override public io.tapstate.spi.store.NestShapeStore nestShapes(io.tapstate.spi.store.PipelineIntentScope scope, String database,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        if (Objects.requireNonNull(original, ORIGINAL_ARGUMENT).executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoCacheNestShapeStore(pipelineAccess(scope, original, requireActive), database);
    }

    @Override public io.tapstate.spi.store.KeyedStateStore operatorState(io.tapstate.spi.store.OperatorPlacement placement) {
        return new MongoCacheOperatorStateStore(pipelineAccess(placement.scope(), null, () -> {}), placement);
    }

    @Override public io.tapstate.spi.store.KeyedStateStore operatorState(io.tapstate.spi.store.OperatorPlacement placement,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        if (Objects.requireNonNull(original, ORIGINAL_ARGUMENT).executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoCacheOperatorStateStore(pipelineAccess(placement.scope(), original, requireActive), placement);
    }

    @Override public io.tapstate.spi.store.KeyedStateStore executionBoundNestState(io.tapstate.spi.store.OperatorPlacement placement,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        if (placement.resource() != io.tapstate.spi.store.OperatorPlacement.Resource.NEST_STATE
                || Objects.requireNonNull(original, ORIGINAL_ARGUMENT).executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoCacheOperatorStateStore(pipelineAccess(placement.scope(), original, requireActive), placement, true);
    }

    @Override public io.tapstate.spi.store.RateHistoryStore pipelineRateHistory(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCacheRateHistoryStore(pipelineAccess(scope, null, () -> {}));
    }
    @Override public io.tapstate.spi.store.RateHistoryStore pipelineRateHistory(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCacheRateHistoryStore(pipelineAccess(scope, Objects.requireNonNull(original, ORIGINAL_ARGUMENT), requireActive));
    }

    @Override public io.tapstate.spi.store.DerivedSchemaStore pipelineDerivedSchemas(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCacheDerivedSchemaStore(pipelineAccess(scope, null, () -> {}));
    }
    /** Authoring model copies precede execution preparation; this view cannot prepare claims, state or model pins. */
    public io.tapstate.spi.store.DerivedSchemaStore unpreparedPipelineDerivedSchemas(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence originalNode, Runnable unassignedConfiguration) {
        Objects.requireNonNull(unassignedConfiguration);
        Runnable guard = () -> { businessActive(); unassignedConfiguration.run(); };
        return new MongoCacheDerivedSchemaStore(MongoCachePipelineAccess.unpreparedModels(client, database, scope, originalNode, guard, rateRetention));
    }
    public io.tapstate.spi.store.DerivedSchemaStore unpreparedPipelineDerivedSchemas(MongoConfigurationDomain.AuthoringModels original,
            io.tapstate.spi.store.WorkloadClaimFence originalNode, Runnable active) {
        Objects.requireNonNull(original); Objects.requireNonNull(active);
        return new MongoCacheDerivedSchemaStore(MongoCachePipelineAccess.unpreparedModels(client, database, original, originalNode,
                () -> { businessActive(); active.run(); }, rateRetention));
    }
    @Override public io.tapstate.spi.store.DerivedSchemaStore pipelineDerivedSchemas(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCacheDerivedSchemaStore(pipelineAccess(scope, Objects.requireNonNull(original, ORIGINAL_ARGUMENT), requireActive));
    }

    /** A fixed read view refuses missing checkpoints rather than interpreting them as a new Pipeline. */
    @Override public io.tapstate.spi.store.StateStore pipelineStates(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCachePipelineStateStore(pipelineAccess(scope, null, () -> {}), claimOperations.structures);
    }

    /** Reads the original checkpoint, prepared claim and business admission in one snapshot. */
    public io.tapstate.core.lifecycle.CheckpointDoc startCheckpoint(io.tapstate.spi.store.PipelineIntentScope scope,
            Runnable requireActive) {
        return pipelineAccess(scope, null, requireActive).startCheckpoint();
    }
    /** Only the original prepared control header, without positions or operator state materialization. */
    public io.tapstate.core.lifecycle.CheckpointDoc validationPipelineState(io.tapstate.spi.store.PipelineIntentScope scope,Runnable local){
        return pipelineAccess(scope,null,Objects.requireNonNull(local)).validationControlTransaction(
                (session,state)->MongoStateStore.toCheckpoint(state));
    }

    /** Read-only evidence for a Stop already completed by this process; never grants execution authority. */
    public boolean stoppedCheckpointUnchanged(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.core.lifecycle.CheckpointDoc original, io.tapstate.spi.store.WorkloadClaimFence fence,
            boolean leaseEnded, Runnable active) {
        Objects.requireNonNull(original); Objects.requireNonNull(fence); Objects.requireNonNull(active);
        if (fence.key().type() != io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION
                || !scope.clusterId().equals(fence.key().clusterId()) || !scope.pipelineId().equals(fence.key().resourceId())) return false;
        var access = pipelineAccess(scope, null, active);
        return access.observationTransaction(session -> {
            var state = access.state(session, true);
            if (!original.equals(MongoStateStore.toCheckpoint(state))
                    || io.tapstate.core.lifecycle.StateJson.parse(original.stateJson()) != io.tapstate.core.lifecycle.PipelineState.STOPPED) return false;
            var id = MongoCachePipelineObservationStore.claimKey(access);
            var reading = MongoCachePipelineObservationStore.reading(session, access, state,
                    MongoCachePipelineObservationStore.claims(session, access, java.util.List.of(id)).get(id));
            if (!fence.equals(io.tapstate.spi.store.WorkloadClaimFence.from(MongoWorkloadClaimStore.decodeStrict(reading.claim())))) return false;
            if (leaseEnded) {
                // The caller retains the completed release receipt in this process. The store
                // proves only its exact original tuple remains unleased; expiry is not a receipt.
                if (MongoWorkloadClaimStore.nodeBacked(reading.claim()) || reading.live()) return false;
            } else if (!reading.live()) return false;
            var row = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(database).find(session,
                    new org.bson.Document("_id", reading.key())).first();
            return MongoCachePipelineObservationStore.select(reading, row, null)
                    .filter(value -> value.state() == io.tapstate.core.lifecycle.PipelineState.STOPPED).isPresent();
        });
    }

    /** A write view retains exactly the original claim; a later pass opens its own view. */
    @Override public io.tapstate.spi.store.StateStore pipelineStates(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCachePipelineStateStore(pipelineAccess(scope, Objects.requireNonNull(original, ORIGINAL_ARGUMENT), requireActive),
                claimOperations.structures);
    }

    /** Read observations and their existing checkpoint and claim fences in one CACHE snapshot. */
    public record PipelineObservationReading(java.util.Optional<io.tapstate.core.lifecycle.Observation> observation,
            boolean runningObservationUnavailable, io.tapstate.core.lifecycle.PipelineState checkpoint) {
        public PipelineObservationReading(java.util.Optional<io.tapstate.core.lifecycle.Observation> observation,
                boolean runningObservationUnavailable) {
            this(observation, runningObservationUnavailable, null);
        }
    }

    public java.util.Map<String, PipelineObservationReading> pipelineObservationBatch(
            java.util.List<io.tapstate.spi.store.PipelineIntentScope> scopes, Runnable active, boolean includeDisplayState) {
        if (scopes.isEmpty()) return java.util.Map.of();
        var first = scopes.getFirst();
        var business = new CacheBusinessScope(first.clusterId(), first.bindingVersion(), first.cacheIncarnation());
        if (scopes.stream().anyMatch(scope -> !business.equals(new CacheBusinessScope(
                scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()))))
            throw MongoCachePipelineAccess.fenced();
        var accesses = scopes.stream().map(scope -> pipelineAccess(scope, null, active)).toList();
        var ids = scopes.stream().map(io.tapstate.spi.store.PipelineIntentScope::pipelineId).toList();
        var keys = accesses.stream().map(MongoCachePipelineObservationStore::claimKey).toList();
        return accesses.getFirst().transaction(session -> {
            var states = observationRows(session, SystemCollections.CACHE_PIPELINE_STATE, ids);
            var claims = MongoCachePipelineObservationStore.claims(session, accesses.getFirst(), keys);
            var readings = new java.util.ArrayList<MongoCachePipelineObservationStore.Reading>();
            var result = new java.util.HashMap<String, PipelineObservationReading>();
            for (var access : accesses) {
                try {
                    readings.add(MongoCachePipelineObservationStore.reading(session, access, states.get(access.scope.pipelineId()),
                            claims.get(MongoCachePipelineObservationStore.claimKey(access))));
                } catch (TapstateException failure) {
                    if (failure.code() != IoError.CACHE_BUSINESS_FENCED
                            && failure.code() != IoError.WORKLOAD_CLAIM_FENCED) throw failure;
                    // The ordinary reader classifies a concurrently replaced scope.
                }
            }
            if (!readings.isEmpty()) {
                var observationKeys = readings.stream().map(MongoCachePipelineObservationStore.Reading::key).toList();
                var observations = observationRows(session, SystemCollections.CACHE_PIPELINE_OBSERVATION, observationKeys);
                var terminal = MongoCachePipelineObservationStore.terminalDetails(session, accesses.getFirst(), readings, observations);
                var unavailable = includeDisplayState
                        ? MongoCachePipelineObservationStore.unavailableRunning(session, accesses.getFirst(), readings, observations)
                        : java.util.Set.<String>of();
                for (var reading : readings) {
                    var observation = MongoCachePipelineObservationStore.select(
                            reading, observations.get(reading.key()), terminal.get(reading.pipeline()));
                    var recovery = includeDisplayState ? MongoStateStore.leaseRecovery(reading.state()) : null;
                    boolean recovering = recovery != null && reading.actual() == io.tapstate.core.lifecycle.PipelineState.RUNNING
                            && (observation.isEmpty() || reading.claim().getLong("executionGeneration") <= recovery.failedExecution());
                    if (recovering) observation = java.util.Optional.empty();
                    result.put(reading.pipeline(), new PipelineObservationReading(
                            observation, observation.isEmpty() && (recovering || unavailable.contains(reading.pipeline())), reading.actual()));
                }
            }
            return java.util.Map.copyOf(result);
        });
    }

    private java.util.Map<Object, Document> observationRows(com.mongodb.client.ClientSession session,
            SystemCollections table, java.util.List<?> ids) {
        var result = new java.util.HashMap<Object, Document>();
        for (var row : table.on(database).find(session, new Document("_id", new Document("$in", ids))))
            result.put(row.get("_id"), row);
        return result;
    }

    public record ObservationSource(String sourceId, long snapshotEpoch, Set<String> tables) {
        public ObservationSource {
            Objects.requireNonNull(sourceId, "sourceId");
            tables = Set.copyOf(tables);
        }
    }

    public record ObservationFacts(io.tapstate.core.lifecycle.CheckpointDoc checkpoint,
            Map<String, io.tapstate.spi.store.ConsumerOffset> cursors, Map<String, Map<String, Long>> counts,
            Map<String, io.tapstate.spi.store.DiscoveredSourceModel> schemas) {
        public ObservationFacts {
            cursors = Map.copyOf(cursors); counts = Map.copyOf(counts); schemas = Map.copyOf(schemas);
        }
    }

    public final class ObservationRead {
        private final ObservationFacts facts;
        private final MongoCachePipelineAccess access;
        private final MongoStructureProof.Verdict proof;

        private ObservationRead(ObservationFacts facts, MongoCachePipelineAccess access, MongoStructureProof.Verdict proof) {
            this.facts = facts;
            this.access = access;
            this.proof = proof;
        }

        public ObservationFacts facts() { return facts; }

        public boolean updateExisting(io.tapstate.core.lifecycle.Observation observation, long epoch) {
            access.requireObservationActive();
            if (!inspectedCollections.get().containsAll(MongoPipelineObservationRead.COLLECTIONS) || !structures.stands(MongoStructureProof.Kind.OBSERVATION, proof)) return false;
            try {
                return new MongoCachePipelineObservationStore(access).updateExisting(observation, epoch);
            } catch (RuntimeException failure) {
                structures.clear();
                access.requireObservationActive();
                throw failure;
            }
        }
    }

    public java.util.Optional<ObservationRead> readObservation(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Map<io.tapstate.spi.store.SrsConsumerRun, ObservationSource> sources,
            Runnable active) {
        var access = pipelineAccess(scope, original, active);
        access.requireObservationActive();
        if (!MongoReadBudget.supportsSnapshotReads(client)
                || "transaction".equals(System.getProperty(MongoSrsLease.VALIDATION_PROPERTY))) return java.util.Optional.empty();
        if (!inspectedCollections.get().containsAll(MongoPipelineObservationRead.COLLECTIONS)) return java.util.Optional.empty();
        try {
            var proof = structures.audit(MongoStructureProof.Kind.OBSERVATION, () -> inspectedCollections.get().containsAll(MongoPipelineObservationRead.COLLECTIONS));
            if (!structures.stands(MongoStructureProof.Kind.OBSERVATION, proof)) return java.util.Optional.empty();
            var facts = new MongoPipelineObservationRead(client, database, access).read(Map.copyOf(sources));
            access.requireObservationActive();
            return facts.map(value -> new ObservationRead(value, access, proof));
        } catch (RuntimeException failure) {
            structures.clear();
            access.requireObservationActive();
            if (failure instanceof TapstateException coded && coded.code() == IoError.DOCUMENT_TOO_LARGE)
                return java.util.Optional.empty();
            throw failure;
        }
    }

    public record ObservationWrite(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active,
            Observation observation, long checkpointEpoch) {}

    /** Writes independent latest observations and returns each original entry's failure, or null on success. */
    public List<RuntimeException> writeObservations(List<ObservationWrite> batch) {
        return MongoCachePipelineObservationStore.writeBatch(batch.stream().map(write ->
                new MongoCachePipelineObservationStore.BatchWrite(
                        new MongoCachePipelineObservationStore(pipelineAccess(write.scope(), write.original(), write.active())),
                        write.observation(), write.checkpointEpoch())).toList());
    }

    /** Missing projections are unknown; they do not permit creation of a missing checkpoint. */
    @Override public io.tapstate.spi.store.ObservationStore pipelineObservations(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCachePipelineObservationStore(pipelineAccess(scope, null, () -> {}));
    }

    /** Normal observations are protected by the same original actuation claim as transitions. */
    @Override public io.tapstate.spi.store.ObservationStore pipelineObservations(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCachePipelineObservationStore(pipelineAccess(scope, Objects.requireNonNull(original, ORIGINAL_ARGUMENT), requireActive));
    }

    private MongoCachePipelineAccess pipelineAccess(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        return new MongoCachePipelineAccess(client, database, scope, original,
                () -> { businessActive(); requireActive.run(); }, rateRetention, operatorDatabases);
    }

    public MongoCachePipelineRunProgressStore pipelineRunProgress(PipelineIntentScope scope,
            WorkloadClaimFence claim, Runnable requireActive) {
        return new MongoCachePipelineRunProgressStore(pipelineAccess(scope, claim, requireActive), claim);
    }

    /** Prepares the optional display resource without placing schema work on the Start path. */
    public void preparePipelineRunProgress(CacheBusinessScope expected, Runnable requireActive) {
        Runnable active = () -> { businessActive(); requireActive.run(); };
        var structure = new MongoCacheMetadataStructure(database, rateRetention);
        StoreIo.run(() -> {
            active.run();
            if (structure.hasPipelineRunProgress()) {
                structure.verifyFacet(SystemCollections.CACHE_PIPELINE_RUN_PROGRESS);
                return;
            }
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    active.run();
                    new MongoCacheBusinessIdentity(database, expected).admitMetadataWrite(session);
                    structure.preparePipelineRunProgressInTransaction(session, active);
                    return null;
                }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.LOCAL)
                        .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build());
            }
        });
    }

    /**
     * Only the original authorized operation may retire a business scope; it does not change auth.
     * Cache fencing does not establish that external calls already issued have drained; the caller
     * retains the existing isolation/drain requirements before publishing another running domain.
     */
    public void retireBusiness(CacheBusinessScope expected, String operationId, Runnable requireActive) {
        Objects.requireNonNull(expected, "scope"); Objects.requireNonNull(requireActive, "requireActive");
        if (operationId == null || operationId.isBlank()) throw new IllegalArgumentException("retirement operation must be fixed");
        StoreIo.run(() -> {
            try {
                businessActive(); requireActive.run();
                try (var session = client.startSession()) {
                    session.withTransaction(() -> {
                        businessActive(); requireActive.run();
                        MongoCacheWorkloadClaimStore.requireCollection(database);
                        new MongoCacheBusinessIdentity(database, expected).retire(session, operationId);
                        businessActive(); requireActive.run();
                        return null;
                    }, businessTransaction());
                }
            } catch (RuntimeException failure) { businessActive(); requireActive.run(); throw failure; }
        });
    }

    /** Rolls back sessions tagged with this departed boot without requiring a healthy business cache. */
    public int endTransactionsLeftOpenBy(String processId) {
        businessActive();
        return connection.endTransactionsLeftOpenBy(processId);
    }

    public Duration rateRetention() { businessActive(); return rateRetention; }

    private void businessActive() { if (closed.get()) throw MongoCacheBusinessIdentity.businessFenced(); }
    private static com.mongodb.TransactionOptions businessTransaction() {
        return com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.SNAPSHOT)
                .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build();
    }

    private MongoCacheIdentity facts(CacheAuthScope scope) {
        return new MongoCacheIdentity(connection.client(), connection.database(), Objects.requireNonNull(scope, "scope"));
    }

    private void verifyAuthCollections(StorageBinding.Mode mode) {
        verifyAuthPresence(mode);
        StoreIo.run(() -> {
            for (var table : authTables(mode)) {
                var existing = table.on(database).listIndexes().into(new ArrayList<>());
                if (existing.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw unreadableIndex();
                for (var declared : table.indexes()) {
                    var found = existing.stream().filter(index -> declared.indexName().equals(index.get("name"))).findFirst().orElse(null);
                    Document keys = new Document(); declared.keys().forEach(key -> keys.put(key, 1));
                    if (found == null || !keys.equals(found.get("key")) || Boolean.TRUE.equals(found.get("unique"))
                            || Boolean.TRUE.equals(found.get("sparse")) || Boolean.TRUE.equals(found.get("hidden"))
                            || found.containsKey("partialFilterExpression")) throw unreadableIndex();
                }
            }
        });
    }

    private void verifyAuthPresence(StorageBinding.Mode mode) {
        StoreIo.run(() -> {
            var tables = new ArrayList<>(authTables(mode));
            tables.add(SystemCollections.CACHE_IDENTITY); tables.add(SystemCollections.CACHE_SCHEMA_MIGRATIONS);
            Set<String> required = tables.stream().map(SystemCollections::collectionName).collect(java.util.stream.Collectors.toSet());
            var actual = MongoCollectionMetadata.matching(database, required);
            if (actual.size() != required.size() || actual.stream().anyMatch(row -> !"collection".equals(row.get("type")))) throw unreadableIndex();
        });
    }

    private static List<SystemCollections> authTables(StorageBinding.Mode mode) {
        return mode == StorageBinding.Mode.OP ? List.of(SystemCollections.OP_SESSIONS)
                : List.of(SystemCollections.CLOUD_SESSIONS, SystemCollections.CLOUD_REVOCATIONS);
    }
    private static TapstateException unreadableIndex() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-auth", "field", "index"), null);
    }
    @Override public synchronized void close() {
        if(connectionClosed)return;
        claimOperations.closeIfIdle();
        MongoCacheAdmissionQueue.unbind(client);
        closed.set(true);
        if (nodeSessionClient != null) nodeSessionClient.close();
        connection.close();
        connectionClosed=true;
    }
}
