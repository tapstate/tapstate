package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.UserStore;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.PipelineLayoutStore;
import io.tapstate.spi.store.PipelineDraftStore;
import io.tapstate.spi.store.PipelineStateMappings;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.StorageBindingStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Opens an already initialized configuration domain without opening any cache or business resource.
 * Bootstrap owns creation of identity, schema and key facts. Ordinary opens only verify them and never
 * run the old shared-database migrations or infer a new installation from missing records.
 */
public final class MongoConfigurationDomain implements AutoCloseable {
    static final String LAYOUT = "three-layer-v1";
    static final int SCHEMA_VERSION = 1;
    private final MongoConnection connection;
    private final MongoConfigurationArtifactStore artifacts;
    private final MongoConfigurationCatalogStore catalog;
    private final PipelineLayoutStore layouts;
    private final PipelineStateMappings pipelineStates;
    private final RequiredNoteStore requiredNotes;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationSecurityStores security;
    private final MongoFirstAdminStore firstAdmin;
    private final MongoConfigurationDraftStore drafts;
    private final PersistentKey configurationKey;
    private final String clusterId;
    private final int installedSchemaVersion;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    MongoConfigurationDomain(MongoConnection connection, String clusterId) {
        this.connection = connection;
        this.clusterId = clusterId;
        MongoDatabase database = connection.database();
        Document identity = StoreIo.call(() -> durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database))
                .find(new Document("_id", "cluster")).first());
        if (identity == null || !clusterId.equals(identity.get("clusterId"))
                || !LAYOUT.equals(identity.get("layoutId"))
                || !(identity.get("bindingVersion") instanceof Long binding) || binding < 0) {
            throw unreadable("cluster", "identity");
        }
        Document schema = StoreIo.call(() -> durable(SystemCollections.SCHEMA_MIGRATIONS.on(database))
                .find(new Document("_id", "config")).first());
        if (schema == null || !(schema.get("installedVersion") instanceof Integer installed) || installed < 1) {
            throw unreadable("config", "installedVersion");
        }
        if (installed > SCHEMA_VERSION) {
            throw new TapstateException(MigrationError.DATA_NEWER_THAN_BINARY,
                    Map.of("installed", String.valueOf(installed), "supported", String.valueOf(SCHEMA_VERSION)), null);
        }
        this.installedSchemaVersion = installed;
        Document version = StoreIo.call(() -> durable(SystemCollections.CONFIGURATION_VERSION.on(database))
                .find(new Document("_id", ConfigurationVersion.ID)).first());
        if (version == null || !(version.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) {
            throw unreadable(ConfigurationVersion.ID, "revision");
        }
        PersistentKey key = new PersistentKey(database, clusterId);
        this.configurationKey = key;
        this.artifacts = new MongoConfigurationArtifactStore(connection.client(), database, key);
        this.requiredNotes = new MongoRequiredNoteStore(connection.client(), database, key);
        this.catalog = new MongoConfigurationCatalogStore(connection.client(), database, key);
        artifacts.verifySourceEnvelopes();
        catalog.verifyEnvelopes();
        this.pipelineStates = new MongoPipelineStateMappings(connection.client(), database);
        this.bindings = new MongoStorageBindingStore(connection.client(), database, key);
        bindings.load();
        this.security = new MongoConfigurationSecurityStores(connection.client(), database, clusterId);
        security.verify();
        this.firstAdmin = new MongoFirstAdminStore(connection.client(), database, bindings);
        firstAdmin.load();
        this.layouts = new MongoPipelineLayoutStore(connection.client(),
                SystemCollections.CONFIG_PIPELINE_LAYOUTS.on(database),
                SystemCollections.CONFIGURATION_VERSION.on(database));
        this.drafts = new MongoConfigurationDraftStore(connection.client(), database, key, artifacts);
        drafts.verify();
        new MongoRequiredDeadLetterStore(connection.client(), database, clusterId).verify();
    }

    /** The caller supplies the trusted Cluster identity from its deployment or installation context. */
    public static MongoConfigurationDomain open(MongoConnectionSettings settings, String expectedClusterId) {
        Objects.requireNonNull(expectedClusterId, "expectedClusterId");
        if (expectedClusterId.isBlank()) throw new IllegalArgumentException("expected Cluster identity must not be blank");
        MongoConnection connection = MongoConnection.forConfiguration(settings);
        try {
            connection.verifyConnectivity();
            return new MongoConfigurationDomain(connection, expectedClusterId);
        } catch (RuntimeException | Error failure) {
            connection.close();
            throw failure;
        }
    }

    /** An OP restart reads its existing identity from the owner-selected configuration endpoint; it never creates one. */
    public static MongoConfigurationDomain openExisting(MongoConnectionSettings settings) {
        MongoConnection connection = MongoConnection.forConfiguration(settings);
        try {
            connection.verifyConnectivity();
            Document identity = StoreIo.call(() -> durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(connection.database()))
                    .find(new Document("_id", "cluster")).first());
            if (identity == null || !(identity.get("clusterId") instanceof String clusterId) || clusterId.isBlank())
                throw unreadable("cluster", "identity");
            return new MongoConfigurationDomain(connection, clusterId);
        } catch (RuntimeException | Error failure) {
            connection.close(); throw failure;
        }
    }

    public String clusterId() { requireOpen(); return clusterId; }
    /** The CONFIG schema fact verified when this domain was opened, not a CACHE readiness signal. */
    public int schemaVersion() { requireOpen(); return installedSchemaVersion; }

    /** Feature schema validation is load-only and does not change the global CONFIG version. */
    public int alertSchemaVersion() {
        var budget = new MongoReadBudget(java.time.Duration.ofSeconds(5));
        requireOpen();
        return new MongoAlertStorageSchema(connection.client(), connection.database(), clusterId,
                configurationKey.refresh(budget).activeKeyId(), this::requireOpen).load(budget);
    }

    /** Explicit additive preparation; ordinary opens and reads never repair lost alert resources. */
    public int prepareAlertStorage(String operationId) {
        var budget = new MongoReadBudget(java.time.Duration.ofSeconds(30));
        requireOpen();
        return new MongoAlertStorageSchema(connection.client(), connection.database(), clusterId,
                configurationKey.refresh(budget).activeKeyId(), this::requireOpen).prepare(operationId, budget);
    }
    public String databaseName() { requireOpen(); return connection.database().getName(); }

    public ArtifactStore artifacts() {
        return artifacts;
    }

    public CatalogStore catalog() {
        return catalog;
    }

    public PipelineLayoutStore layouts() {
        return layouts;
    }

    public RequiredNoteStore requiredNotes() {
        return requiredNotes;
    }

    public PipelineStateMappings pipelineStates() {
        return pipelineStates;
    }

    public io.tapstate.spi.store.PipelineRetirements retirements() {
        requireOpen();return new MongoPipelineRetirements(connection.client(),connection.database(),clusterId,bindings,artifacts,
                (MongoRequiredNoteStore)requiredNotes,this::requireOpen);
    }
    public void requireRetiredPipelinePredecessor(io.tapstate.spi.store.PipelineIntentScope current, io.tapstate.spi.store.PipelineIntentScope earlier) {
        requireOpen(); new MongoPipelineStatePreparations(connection.client(),connection.database(),clusterId,bindings,artifacts,this::requireOpen)
                .requireRetiredPredecessor(current,earlier);
    }

    public io.tapstate.spi.store.CaptureStatePreparations capturePreparations() {
        requireOpen();return new MongoCaptureStatePreparations(connection.client(),connection.database(),clusterId,bindings,artifacts,this::requireOpen);
    }

    public io.tapstate.spi.store.PipelineStatePreparations preparations() {
        requireOpen();
        return new MongoPipelineStatePreparations(connection.client(), connection.database(), clusterId, bindings, artifacts, this::requireOpen);
    }
    /** A native, load-only proof for model copies before this exact physical generation is prepared. */
    public AuthoringModels authoringModels(io.tapstate.spi.store.PipelineIntentScope scope) {
        requireOpen();
        var previous = ((MongoPipelineStatePreparations) preparations()).authoringPredecessor(scope);
        var recovery=cacheRecoveries().read(scope.pipelineId());
        if(recovery.isPresent() && !recovery.orElseThrow().targetScope().equals(scope))throw MongoCachePipelineAccess.fenced();
        return new AuthoringModels(this, scope, previous,recovery);
    }
    public static final class AuthoringModels {
        private final MongoConfigurationDomain owner;
        private final io.tapstate.spi.store.PipelineIntentScope scope;
        private final java.util.Optional<io.tapstate.spi.store.PipelineRetirementResult> predecessor;
        private final java.util.Optional<io.tapstate.spi.store.PipelineCacheRecovery> recovery;
        private AuthoringModels(MongoConfigurationDomain owner, io.tapstate.spi.store.PipelineIntentScope scope,
                java.util.Optional<io.tapstate.spi.store.PipelineRetirementResult> predecessor,
                java.util.Optional<io.tapstate.spi.store.PipelineCacheRecovery> recovery) {
            this.owner = owner; this.scope = scope; this.predecessor = predecessor;this.recovery=recovery;
        }
        public io.tapstate.spi.store.PipelineIntentScope scope() { return scope; }
        java.util.Optional<io.tapstate.spi.store.PipelineRetirementResult> predecessor() { return predecessor; }
        java.util.Optional<io.tapstate.spi.store.PipelineCacheRecovery> recovery() {return recovery;}
        void requireActive() {
            owner.requireOpen();
            if (!predecessor.equals(((MongoPipelineStatePreparations) owner.preparations()).authoringPredecessor(scope))
                    || !recovery.equals(owner.cacheRecoveries().read(scope.pipelineId())))
                throw MongoCachePipelineAccess.fenced();
        }
        @Override public String toString() { return "AuthoringModels[original]"; }
    }

    public long configurationRevision() {
        requireOpen();
        return StoreIo.call(() -> {
            try (var session = connection.client().startSession()) {
                return session.withTransaction(() -> {
                    requireOpen();
                    return ConfigurationVersion.read(session, SystemCollections.CONFIGURATION_VERSION.on(connection.database()));
                });
            }
        });
    }

    public io.tapstate.spi.store.StorageRecoveryCommands recoveryCommands() {
        requireOpen();
        return new MongoStorageRecoveryCommands(connection.client(), connection.database(), bindings, artifacts, this::requireOpen);
    }
    public io.tapstate.spi.store.SharedSrsRecoveryPreparations sharedRecoveryPreparations() {
        requireOpen();return new MongoStorageRecoveryCommands(connection.client(),connection.database(),bindings,artifacts,this::requireOpen);
    }
    public io.tapstate.spi.store.CacheRecoveryStore cacheRecoveries() {
        requireOpen();
        return new MongoCacheRecoveryStore(connection.client(), connection.database(), bindings, artifacts, clusterId, this::requireOpen);
    }

    public void requireOriginalInitialPreparation(io.tapstate.spi.store.CachePreparation original) {
        requireOpen(); bindings.requireOriginalInitialPreparation(original);
    }

    public io.tapstate.core.model.SourceResource initialOpRecoverySource(io.tapstate.spi.store.CacheAuthScope scope, boolean allowCompleted) {
        requireOpen();
        return StoreIo.call(() -> {
            try (var session = connection.client().startSession()) {
                return session.withTransaction(() -> MongoInitialOpRecovery.require(connection.client(), session,
                        connection.database(), bindings, artifacts, scope, allowCompleted));
            }
        });
    }

    /** An unforgeable committed winner, not a DTO that a caller can reconstruct from RUNNING. */
    public static final class InitialOpRecovery {
        private final MongoConfigurationDomain owner;
        private final io.tapstate.spi.store.StorageRecoveryCommands.Plan plan;
        private final boolean first;
        private final java.util.concurrent.atomic.AtomicBoolean attempted = new java.util.concurrent.atomic.AtomicBoolean();
        private InitialOpRecovery(MongoConfigurationDomain owner, io.tapstate.spi.store.StorageRecoveryCommands.Plan plan, boolean first) {
            this.owner = owner; this.plan = plan; this.first = first;
        }
        public io.tapstate.spi.store.StorageRecoveryCommands.Plan plan() { return plan; }
        boolean mayCreate() { return first && !attempted.get(); }
        @Override public String toString() { return "InitialOpRecovery[owned]"; }
    }

    public InitialOpRecovery startInitialOpRecovery(io.tapstate.spi.store.StorageRecoveryCommands.Plan original, Runnable active) {
        requireOpen(); active.run();
        var commands = new MongoStorageRecoveryCommands(connection.client(), connection.database(), bindings, artifacts,
                () -> { requireOpen(); active.run(); });
        var result = commands.startInitialOp(original);
        // A transaction retry carries its own result. Only the confirmed committed callback wins.
        return new InitialOpRecovery(this, result.plan(), result.won());
    }

    public void prepareInitialOpRecovery(MongoCacheDomain cache, InitialOpRecovery invocation, Runnable active) {
        requireOpen();
        if (invocation == null || invocation.owner != this) throw MongoCachePipelineAccess.fenced();
        var running = invocation.plan;
        Runnable original = () -> requireInitialOpOperation(running, active);
        original.run(); cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        var preparation = new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope());
        if (running.phase() == io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING) {
            if (invocation.first && invocation.attempted.compareAndSet(false, true)) cache.prepareAuth(preparation.authScope(), preparation.operationId(), original);
            else cache.resumeInitialAuth(preparation, original);
            cache.prepareConfirmedInitialBusiness(preparation, original);
        } else if (running.phase() == io.tapstate.spi.store.StorageRecoveryCommands.Phase.COMPLETE) {
            cache.verifyInitialRecovery(preparation);
        } else throw MongoCachePipelineAccess.fenced();
        original.run();
    }

    private void requireInitialOpOperation(io.tapstate.spi.store.StorageRecoveryCommands.Plan original, Runnable active) {
        requireOpen(); active.run();
        if (original.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING
                && original.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.COMPLETE) throw MongoCachePipelineAccess.fenced();
        StoreIo.run(() -> {
            try (var session = connection.client().startSession()) {
                session.withTransaction(() -> {
                    var row = SystemCollections.STORAGE_RECOVERY.on(connection.database()).find(session, new Document("_id", "cluster")).first();
                    if (row == null || MongoStorageRecoveryCommands.pending(row).filter(original::equals).isEmpty()) throw MongoCachePipelineAccess.fenced();
                    MongoInitialOpRecovery.requireManifest(connection.client(), session, connection.database(), bindings, artifacts,
                            original.originalScope(), original.manifest(), true);
                    return null;
                });
            }
        });
        requireOpen(); active.run();
    }

    /** Reads the original initial-only closure without preparing or starting any CACHE work. */
    public io.tapstate.core.model.SourceResource initialCloudRecoverySource(io.tapstate.spi.store.CacheAuthScope scope) {
        requireOpen();
        return StoreIo.call(() -> {
            try (var session = connection.client().startSession()) {
                return session.withTransaction(() -> {
                    requireOpen();
                    return MongoInitialCloudRecovery.require(session, connection.database(), bindings, artifacts, scope);
                });
            }
        });
    }

    /** Prepares only the business domain after atomic all-stopped publication and caller-proven old-writer isolation. */
    public void prepareStoppedRecovery(MongoCacheDomain cache, io.tapstate.spi.store.StorageRecoveryCommands.Plan running,
            Runnable localIsolationGuard) {
        requireOpen();localIsolationGuard.run();
        var recoveries=(MongoCacheRecoveryStore)cacheRecoveries();
        recoveries.requireStopped(running);
        cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        // CONFIG validation stays outside CACHE transactions; their callbacks only check this local lifecycle.
        cache.prepareConfirmedInitialBusiness(new io.tapstate.spi.store.CachePreparation(running.operationId(),running.originalScope()),
                () -> {requireOpen();localIsolationGuard.run();});
        recoveries.requireStopped(running);localIsolationGuard.run();requireOpen();
    }

    /** Only the exact current confirmed runtime operation can authorize an initial business birth. */
    public void prepareInitialCloudRecovery(MongoCacheDomain cache, io.tapstate.spi.store.StorageRecoveryCommands.Plan running,
            Runnable requireActive) {
        Runnable original = () -> {
            requireOpen(); requireActive.run();
            StoreIo.run(() -> {
                try (var session = connection.client().startSession()) {
                    session.withTransaction(() -> {
                        var root = SystemCollections.STORAGE_RECOVERY.on(connection.database()).find(session, new Document("_id", "cluster")).first();
                        if (root == null || !MongoStorageRecoveryCommands.pending(root).filter(running::equals).isPresent()
                                || running.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING) throw MongoCachePipelineAccess.fenced();
                        MongoInitialCloudRecovery.requireManifest(session, connection.database(), bindings, artifacts,
                                running.originalScope(), running.manifest());
                        return null;
                    });
                }
            });
        };
        original.run(); cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        cache.prepareConfirmedInitialBusiness(new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope()), original);
        original.run();
    }
    /** Inspects the exact original endpoint without disturbing a still-owned business runtime. */
    public boolean authenticationRecoveryRequired(MongoCacheDomain candidate, Runnable active) {
        requireOpen(); Objects.requireNonNull(candidate); Objects.requireNonNull(active); active.run();
        var original = bindings.authenticationRecovery();
        candidate.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        if (original.pending()) return true;
        if (candidate.emptyForAuthenticationRecovery()) {
            candidate.verifyRecoveryPermissions(); active.run(); return true;
        }
        candidate.loadAuthScope(original.preparation().authScope());
        bindings.requireAuthenticationPublished(original.preparation()); active.run(); return false;
    }

    /** Internal startup continuation only: this prepares no business structure and never changes the saved endpoint. */
    public io.tapstate.spi.store.CachePreparation prepareAuthenticationRecovery(MongoCacheDomain candidate, Runnable active) {
        requireOpen(); Objects.requireNonNull(candidate); Objects.requireNonNull(active);
        active.run();
        var original = bindings.authenticationRecovery();
        var bound = bindings.load().orElseThrow(MongoCachePipelineAccess::fenced);
        candidate.requireRecoveryTarget(bound);
        if (!original.pending()) {
            if (!candidate.emptyForAuthenticationRecovery()) {
                candidate.loadAuthScope(original.preparation().authScope());
                bindings.requireAuthenticationPublished(original.preparation()); active.run(); return original.preparation();
            }
            candidate.verifyRecoveryPermissions(); active.run();
            original = bindings.reserveAuthenticationRecovery(original.preparation());
            if (!original.pending()) {
                candidate.loadAuthScope(original.preparation().authScope());
                bindings.requireAuthenticationPublished(original.preparation()); active.run(); return original.preparation();
            }
        }
        var expected = original.preparation();
        Runnable pending = () -> { requireOpen(); active.run(); bindings.requireAuthenticationRecoveryPending(expected); };
        candidate.prepareRecoveryAuthentication(expected, pending);
        var published = bindings.publishAuthenticationRecovery(expected);
        candidate.loadAuthScope(published.authScope()); bindings.requireAuthenticationPublished(published); active.run();
        return published;
    }
    public void requireAuthenticationPublished(io.tapstate.spi.store.CachePreparation original) {
        requireOpen(); bindings.requireAuthenticationPublished(Objects.requireNonNull(original));
    }

    public void requireAuthenticationPublished(io.tapstate.spi.store.CachePreparation original, java.time.Duration budget) {
        requireOpen(); bindings.requireAuthenticationPublished(Objects.requireNonNull(original), budget);
    }
    public void requireAuthenticationPublished(io.tapstate.spi.store.CachePreparation original, MongoReadBudget budget) {
        requireOpen(); bindings.requireAuthenticationPublished(Objects.requireNonNull(original), Objects.requireNonNull(budget));
    }

    public io.tapstate.spi.store.StorageSetupStateStore setupStates() {
        requireOpen();
        return new MongoStorageSetupStateStore(connection.client(), connection.database(), bindings, this::requireOpen);
    }

    public StorageBindingStore bindings() {
        return bindings;
    }

    public io.tapstate.spi.store.PipelineIntentStore intents() {
        requireOpen();
        return new MongoPipelineIntentStore(connection.client(), connection.database(), clusterId, bindings).guardedBy(this::requireOpen);
    }

    public io.tapstate.spi.store.PipelineStateRebuilds rebuilds() {
        requireOpen();
        return new MongoPipelineStateRebuilds(connection.client(), connection.database(), clusterId, bindings, artifacts, this::requireOpen);
    }

    public io.tapstate.spi.store.ConfigurationExportStore exports() {
        requireOpen();
        return new MongoConfigurationExportStore(connection.client(), connection.database(), clusterId, bindings, artifacts, this::requireOpen);
    }

    public io.tapstate.spi.store.ConfigurationImportStore imports() {
        requireOpen();
        return new MongoConfigurationImportStore(connection.client(),connection.database(),clusterId,bindings,artifacts,this::requireOpen);
    }

    public PipelineDraftStore drafts() { return drafts; }

    public io.tapstate.spi.store.RequiredDeadLetterStore requiredDeadLetters() {
        return new MongoRequiredDeadLetterStore(connection.client(), connection.database(), clusterId);
    }

    public FirstAdminStore firstAdmin() { return firstAdmin; }

    public UserStore users() { return firstAdmin.users(); }

    public TokenStore tokens() { return security.tokens(); }
    public TokenStore tokens(MongoReadBudget budget) { requireOpen(); return security.tokens(Objects.requireNonNull(budget)); }

    public AuditStore audit() { return security.audit(); }

    public ClusterIdentityStore clusterIdentity() { return security.clusterIdentity(); }

    /** Rolls back sessions tagged with this departed boot on the original configuration server. */
    public int endTransactionsLeftOpenBy(String processId) {
        requireOpen();
        return connection.endTransactionsLeftOpenBy(processId);
    }

    @Override
    public void close() {
        closed.set(true);
        connection.close();
    }

    private void requireOpen() {
        if (closed.get()) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }

    /** One persistent key, with no node-session ACK, rotation state or cache access. */
    private static final class PersistentKey implements SourceConfigCipherProvider {
        private final MongoCollection<Document> keys;
        private final String clusterId;
        private final SourceConfigCipher cipher;

        private PersistentKey(MongoDatabase database, String clusterId) {
            this.keys = durable(SystemCollections.CLUSTER_KEYS.on(database));
            this.clusterId = clusterId;
            this.cipher = StoreIo.call(() -> decode(keys.find(new Document("_id", "cluster")).first()));
        }

        @Override
        public SourceConfigCipher current() {
            return cipher;
        }

        @Override
        public SourceConfigCipher refresh() {
            SourceConfigCipher observed = StoreIo.call(() -> decode(keys.find(new Document("_id", "cluster")).first()));
            if (!cipher.activeKeyId().equals(observed.activeKeyId())) throw invalidKey();
            return observed;
        }

        SourceConfigCipher refresh(MongoReadBudget budget) {
            SourceConfigCipher observed = StoreIo.call(() -> {
                SourceConfigCipher value = decode(keys.withTimeout(budget.operationMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                        .find(new Document("_id", "cluster")).first());
                budget.remainingMillis();
                return value;
            });
            if (!cipher.activeKeyId().equals(observed.activeKeyId())) throw invalidKey();
            return observed;
        }

        @Override
        public boolean requiresWriteFence() {
            return true;
        }

        @Override
        public boolean fenceWrite(SourceConfigWriteScope scope, String keyId) {
            SourceConfigCipher observed = decode(keys.find(scope.session(), new Document("_id", "cluster")).first());
            if (!cipher.activeKeyId().equals(keyId) || !keyId.equals(observed.activeKeyId())) throw invalidKey();
            return keys.updateOne(scope.session(), new Document("_id", "cluster").append("keyId", keyId),
                    new Document("$inc", new Document("sourceWriteFence", 1L))).getMatchedCount() == 1;
        }

        private SourceConfigCipher decode(Document document) {
            if (document == null || !clusterId.equals(document.get("clusterId"))
                    || !Integer.valueOf(1).equals(document.get("formatVersion"))
                    || !(document.get("keyId") instanceof String keyId)
                    || !(document.get("material") instanceof String material)) throw invalidKey();
            byte[] bytes;
            try {
                bytes = Base64.getUrlDecoder().decode(material);
            } catch (IllegalArgumentException invalid) {
                throw invalidKey();
            }
            try {
                if (bytes.length != 32 || !keyId.equals(SourceConfigCipher.fingerprint(bytes))) throw invalidKey();
                return new SourceConfigCipher(bytes);
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }

        private static TapstateException invalidKey() {
            return new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
        }
    }
}
