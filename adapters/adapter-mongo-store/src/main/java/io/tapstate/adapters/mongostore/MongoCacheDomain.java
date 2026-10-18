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
public final class MongoCacheDomain implements io.tapstate.spi.store.PipelineStoreViews, AutoCloseable {
    private final MongoConnection connection;
    private final com.mongodb.client.MongoClient client;
    private final com.mongodb.client.MongoDatabase database;
    private final Duration rateRetention;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    private MongoCacheDomain(MongoConnection connection, Duration rateRetention) {
        this.connection = connection; this.client = connection.client(); this.database = connection.database(); this.rateRetention = rateRetention;
    }

    /** Connectivity alone creates no identity, schema, session, membership or business resource. */
    public static MongoCacheDomain connect(StorageBinding.Connection cache, Duration timeout, String processId) {
        return connect(cache, timeout, processId, MongoRateHistoryStore.DEFAULT_RETENTION);
    }

    public static MongoCacheDomain connect(StorageBinding.Connection cache, Duration timeout, String processId, Duration rateRetention) {
        rateRetention = MongoCacheMetadataStructure.requireRetention(rateRetention);
        MongoConnection connection = MongoConnection.forCache(cache, timeout, processId);
        try {
            connection.verifyConnectivity();
            return new MongoCacheDomain(connection, rateRetention);
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

    /** The scope was durably allocated by the original configuration setup or recovery operation. */
    public CacheAuthScope prepareAuth(CacheAuthScope scope, String operationId, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        requireActive.run();
        boolean emptyTarget = StoreIo.call(() -> {
            try (var names = connection.database().listCollectionNames().iterator()) { return !names.hasNext(); }
        });
        var facts = facts(scope);
        facts.beginPreparation(operationId, emptyTarget, requireActive);
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

    /** Verifies a fixed business identity and its schema; individual facets verify their own structures. */
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
        return new MongoCachePipelineAccess(client, database, scope, original, () -> { businessActive(); requireActive.run(); }, rateRetention);
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
                var existing = table.on(connection.database()).listIndexes().into(new ArrayList<>());
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
            var actual = connection.database().listCollections().filter(new Document("name", new Document("$in", new ArrayList<>(required)))).into(new ArrayList<>());
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
