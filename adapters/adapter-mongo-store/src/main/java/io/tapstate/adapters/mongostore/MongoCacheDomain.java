package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.WorkloadClaimStore;
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

/** One cache client owner, with authentication preparation independent from business-domain assembly. */
public final class MongoCacheDomain implements io.tapstate.spi.store.PipelineStoreViews, io.tapstate.spi.store.SrsStoreViews, AutoCloseable {
    private final MongoConnection connection;
    private final com.mongodb.client.MongoClient client;
    private final com.mongodb.client.MongoDatabase database;
    private final Duration rateRetention;
    private final io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases;
    private final String targetFingerprint;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    private MongoCacheDomain(MongoConnection connection, Duration rateRetention, String targetFingerprint,
                             io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases) {
        this.operatorDatabases = operatorDatabases;
        this.connection = connection; this.client = connection.client(); this.database = connection.database(); this.rateRetention = rateRetention; this.targetFingerprint = targetFingerprint;
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

    /** Ordinary load verifies only the original caller-supplied authentication identity and schema. */
    public CacheAuthScope loadAuthScope(CacheAuthScope expected) {
        var facts = facts(expected);
        facts.transaction(session -> { facts.requireReady(session); return null; });
        verifyAuthCollections(expected.mode());
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
        verifyAuthCollections(expected.mode(), budget);
        budget.requireRemaining();
        return expected;
    }
    void requireRecoveryTarget(StorageBinding binding) {
        businessActive();
        if (!targetFingerprint.equals(MongoStorageBindingStore.targetFingerprint(binding.cache()))) throw MongoCacheIdentity.fenced();
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
        Runnable active = () -> { businessActive(); original.requireActive(); };
        active.run();
        boolean emptyTarget = StoreIo.call(() -> { try (var names = database.listCollectionNames().iterator()) { return !names.hasNext(); } });
        var facts = facts(preparation.authScope());
        facts.beginFirstPreparation(preparation.operationId(), emptyTarget, active);
        return finishAuth(facts, preparation.authScope(), preparation.operationId(), active);
    }

    /** A controlled continuation may complete retained facts but never recreate a missing identity. */
    public CacheAuthScope resumeInitialAuth(io.tapstate.spi.store.CachePreparation original, Runnable requireActive) {
        Objects.requireNonNull(original, "original"); Objects.requireNonNull(requireActive, "requireActive");
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
        return new MongoCacheSessionStore(facts(scope), connection.database(), () -> verifyAuthPresence(scope.mode()));
    }

    /** A managed-session view does not adopt a later authentication generation. */
    public CloudSessionStore cloudSessions(CacheAuthScope scope) {
        if (scope.mode() != StorageBinding.Mode.CLOUD) throw MongoCacheIdentity.fenced();
        loadAuthScope(scope);
        return new MongoCacheCloudSessionStore(facts(scope), connection.database(), () -> verifyAuthPresence(scope.mode()));
    }

    /** A request-owned view retains its scope and deadline; each operation checks structures and fences. */
    public CloudSessionStore cloudSessions(CacheAuthScope scope, MongoReadBudget budget) {
        if (scope.mode() != StorageBinding.Mode.CLOUD) throw MongoCacheIdentity.fenced();
        Objects.requireNonNull(budget, "budget").requireRemaining();
        return new MongoCacheCloudSessionStore(facts(scope), connection.database(),
                () -> verifyAuthPresence(scope.mode(), budget), budget);
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

    /** Admission validates retained business structures and configured indexes without repairing them. */
    public void verifyBusinessReady(CacheBusinessScope expected) {
        loadBusinessScope(expected);
        new MongoCacheMetadataStructure(database, rateRetention).verifyBusiness();
        loadBusinessScope(expected);
    }

    /** Controlled continuation requires the original operation and already retained authentication facts. */
    public CacheBusinessScope prepareInitialBusiness(io.tapstate.spi.store.CachePreparation original, Runnable requireActive) {
        Objects.requireNonNull(original, "original"); Objects.requireNonNull(requireActive, "requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run();
        return new MongoCacheBusinessPreparation(client, database, original, rateRetention, active).prepare();
    }

    MongoValidationCachePreparations validationPreparations(CacheBusinessScope expected) {
        loadBusinessScope(expected);
        return new MongoValidationCachePreparations(client, database, expected, targetFingerprint, this::businessActive);
    }

    /** The claims share this exact cache client, original scope and owner lifecycle. */
    public WorkloadClaimStore workloadClaims(CacheBusinessScope expected) {
        loadBusinessScope(expected);
        StoreIo.run(() -> MongoCacheWorkloadClaimStore.requireCollection(database));
        return new MongoCacheWorkloadClaimStore(client, database, expected, this::businessActive);
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
        var structure = new MongoCacheMetadataStructure(database, rateRetention); structure.verifySrs();
        return new MongoSrsConsumerStore(client, database, expected, active, structure::verifySrs).positions(node, rateRetention);
    }

    /** SRS facets retain one CACHE client, complete business scope and original caller lifecycle. */
    @Override public io.tapstate.spi.store.SrsStores srs(CacheBusinessScope expected, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run(); loadBusinessScope(expected);
        var structure = new MongoCacheMetadataStructure(database, rateRetention);
        structure.verifySrs(); active.run();
        var models = new MongoSrsModelStore(client, database, expected, active, structure::verifySrs);
        return new io.tapstate.spi.store.SrsStores(
                new MongoSrsChainStore(client, database, expected, active, structure::verifySrs), models,
                new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models),
                new MongoSrsConsumerStore(client, database, expected, active, structure::verifySrs));
    }

    @Override public io.tapstate.spi.store.SnapshotStateStore snapshotStates(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original,Runnable requireActive) {
        if (Objects.requireNonNull(original,"original").executionGeneration()<1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoSnapshotStateStore(pipelineAccess(scope,original,requireActive));
    }

    /** The private direct-channel view retains this exact CACHE identity, client and lifecycle. */
    @Override public io.tapstate.spi.store.DirectChannelStore directChannels(CacheBusinessScope expected,Runnable requireActive) {
        Objects.requireNonNull(requireActive,"requireActive");
        Runnable active = () -> { businessActive(); requireActive.run(); };
        active.run(); loadBusinessScope(expected); new MongoCacheMetadataStructure(database,rateRetention).verifyDirect();
        return new MongoDirectChannelStore(client,database,expected,active,rateRetention);
    }

    public io.tapstate.spi.store.SchemaStore schemas(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheSchemaStore(metadataAccess(expected, SystemCollections.CACHE_SOURCE_SCHEMAS, requireActive));
    }

    public io.tapstate.spi.store.ConnectorRegistry connectors(CacheBusinessScope expected, Runnable requireActive) {
        var registry = new MongoCacheConnectorRegistry(client, database, expected, this::businessActive, requireActive);
        registry.verify(); return registry;
    }

    public io.tapstate.spi.store.ConnectorCatalogStore connectorCatalog(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectorCatalogStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTOR_CATALOG, requireActive));
    }
    public io.tapstate.spi.store.ConnectorSpecStore connectorSpecs(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectorSpecStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTOR_SPECS, requireActive));
    }
    public io.tapstate.spi.store.ConnectionTestResultStore connectionTestResults(CacheBusinessScope expected, Runnable requireActive) {
        return new MongoCacheConnectionTestResultStore(metadataAccess(expected, SystemCollections.CACHE_CONNECTION_TEST_RESULTS, requireActive));
    }
    private MongoCacheMetadataAccess metadataAccess(CacheBusinessScope expected, SystemCollections table, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        var access = new MongoCacheMetadataAccess(client, database, expected, table, () -> { businessActive(); requireActive.run(); });
        access.verify(); return access;
    }

    /** Controlled first preparation retains this exact cache domain and caller lifecycle. */
    public io.tapstate.spi.store.PipelineCachePreparations pipelinePreparations(CacheBusinessScope expected, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        return new MongoCachePipelinePreparations(client, database, expected, () -> { businessActive(); requireActive.run(); });
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
        if (Objects.requireNonNull(original, "original").executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        MongoCachePipelineAccess access = pipelineAccess(scope, original, requireActive);
        var target = access.operatorDatabase(targetDatabase, io.tapstate.spi.store.OperatorPlacement.Resource.NEST_STATE);
        if (target.getName().equals(database.getName())) {
            access.metadataTransaction(SystemCollections.CACHE_NEST_STATE, true, session -> {
                new MongoCacheMetadataStructure(database, rateRetention).verifyFacet(SystemCollections.CACHE_NEST_SHAPES);
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
        if (Objects.requireNonNull(original, "original").executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoCacheNestShapeStore(pipelineAccess(scope, original, requireActive), database);
    }

    @Override public io.tapstate.spi.store.KeyedStateStore operatorState(io.tapstate.spi.store.OperatorPlacement placement) {
        return new MongoCacheOperatorStateStore(pipelineAccess(placement.scope(), null, () -> {}), placement);
    }

    @Override public io.tapstate.spi.store.KeyedStateStore operatorState(io.tapstate.spi.store.OperatorPlacement placement,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        if (Objects.requireNonNull(original, "original").executionGeneration() < 1) throw MongoCachePipelineAccess.claimFenced();
        return new MongoCacheOperatorStateStore(pipelineAccess(placement.scope(), original, requireActive), placement);
    }

    @Override public io.tapstate.spi.store.RateHistoryStore pipelineRateHistory(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCacheRateHistoryStore(pipelineAccess(scope, null, () -> {}));
    }
    @Override public io.tapstate.spi.store.RateHistoryStore pipelineRateHistory(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCacheRateHistoryStore(pipelineAccess(scope, Objects.requireNonNull(original, "original"), requireActive));
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
        return new MongoCacheDerivedSchemaStore(pipelineAccess(scope, Objects.requireNonNull(original, "original"), requireActive));
    }

    /** A fixed read view refuses missing checkpoints rather than interpreting them as a new Pipeline. */
    @Override public io.tapstate.spi.store.StateStore pipelineStates(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCachePipelineStateStore(pipelineAccess(scope, null, () -> {}));
    }

    /** A write view retains exactly the original claim; a later pass opens its own view. */
    @Override public io.tapstate.spi.store.StateStore pipelineStates(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCachePipelineStateStore(pipelineAccess(scope, Objects.requireNonNull(original, "original"), requireActive));
    }

    /** Missing projections are unknown; they do not permit creation of a missing checkpoint. */
    @Override public io.tapstate.spi.store.ObservationStore pipelineObservations(io.tapstate.spi.store.PipelineIntentScope scope) {
        return new MongoCachePipelineObservationStore(pipelineAccess(scope, null, () -> {}));
    }

    /** Normal observations are protected by the same original actuation claim as transitions. */
    @Override public io.tapstate.spi.store.ObservationStore pipelineObservations(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        return new MongoCachePipelineObservationStore(pipelineAccess(scope, Objects.requireNonNull(original, "original"), requireActive));
    }

    private MongoCachePipelineAccess pipelineAccess(io.tapstate.spi.store.PipelineIntentScope scope,
            io.tapstate.spi.store.WorkloadClaimFence original, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        return new MongoCachePipelineAccess(client, database, scope, original, () -> { businessActive(); requireActive.run(); }, rateRetention, operatorDatabases);
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

    private void verifyAuthCollections(StorageBinding.Mode mode) { verifyAuthCollections(mode, null); }

    private void verifyAuthCollections(StorageBinding.Mode mode, MongoReadBudget budget) {
        verifyAuthPresence(mode, budget);
        StoreIo.run(() -> {
            for (var table : authTables(mode)) {
                var database = budget == null ? connection.database() : budget.database(connection.database());
                var indexes = table.on(database).listIndexes();
                if (budget != null) indexes.timeoutMode(com.mongodb.client.cursor.TimeoutMode.CURSOR_LIFETIME);
                var existing = indexes.into(new ArrayList<>());
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

    private void verifyAuthPresence(StorageBinding.Mode mode) { verifyAuthPresence(mode, null); }

    private void verifyAuthPresence(StorageBinding.Mode mode, MongoReadBudget budget) {
        StoreIo.run(() -> {
            var tables = new ArrayList<>(authTables(mode));
            tables.add(SystemCollections.CACHE_IDENTITY); tables.add(SystemCollections.CACHE_SCHEMA_MIGRATIONS);
            Set<String> required = tables.stream().map(SystemCollections::collectionName).collect(java.util.stream.Collectors.toSet());
            var database = budget == null ? connection.database() : budget.database(connection.database());
            var actual = MongoCollectionMetadata.matching(database, required, budget != null);
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
    @Override public void close() { if (closed.compareAndSet(false, true)) connection.close(); }
}
