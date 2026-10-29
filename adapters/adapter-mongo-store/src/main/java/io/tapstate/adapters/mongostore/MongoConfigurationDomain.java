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
    static final int SCHEMA_VERSION = 3;
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
    private final String clusterId;
    private final int installedSchemaVersion;
    private MongoValidationPreparations validationPreparations;
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
        if (StoreIo.call(() -> MongoValidationSchema.configurationHasValidation(database, schema)))
            StoreIo.run(() -> MongoValidationSchema.requireReady(database, true));
        Document version = StoreIo.call(() -> durable(SystemCollections.CONFIGURATION_VERSION.on(database))
                .find(new Document("_id", ConfigurationVersion.ID)).first());
        if (version == null || !(version.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) {
            throw unreadable(ConfigurationVersion.ID, "revision");
        }
        PersistentKey key = new PersistentKey(database, clusterId);
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
    public String databaseName() { requireOpen(); return connection.database().getName(); }

    /** Explicit offline maintenance only. The operator must stop old writers before opening it. */
    public static ValidationSchemaPreparation prepareValidationSchema(MongoConnectionSettings settings, String clusterId, java.util.UUID operation) {
        return ValidationSchemaPreparation.open(settings, clusterId, operation, true);
    }

    /** Resume only the original retained maintenance operation; missing version-2 facts are never recreated. */
    public static ValidationSchemaPreparation resumeValidationSchema(MongoConnectionSettings settings, String clusterId, java.util.UUID operation) {
        return ValidationSchemaPreparation.open(settings, clusterId, operation, false);
    }

    /**
     * Installer-owned continuation before the first business runtime. An existing version-one
     * installation is never upgraded here: the initial transition requires no Pipeline mappings and
     * no live CACHE workers. Replays bind the same initial operation and verify retained resources;
     * they cannot reconstruct a missing version-two collection. Ordinary open never calls this.
     * local is a pure, nonblocking lifecycle guard, not another store or network callback.
     */
    public ValidationSchemaStatus prepareInitialValidationSchema(io.tapstate.spi.store.CachePreparation original,
            MongoCacheDomain cache, Runnable local) {
        requireOpen(); Objects.requireNonNull(original); Objects.requireNonNull(cache); Objects.requireNonNull(local).run();
        if (original.authScope().authGeneration() != 1 || !clusterId.equals(original.authScope().clusterId())) throw MongoValidationSchema.refused();
        bindings.requireOriginalInitialPreparation(original);
        var operation = initialValidationSchemaOperation(original);
        try (var preparation = ValidationSchemaPreparation.open(connection.configurationSettings(), clusterId, operation, true, original,
                () -> { requireOpen(); local.run(); })) {
            var result = preparation.prepareInitial(cache);
            if (!result.ready() || result.initializedPipelines() != 0) throw MongoValidationSchema.refused();
            bindings.requireOriginalInitialPreparation(original); local.run(); return result;
        }
    }

    static java.util.UUID initialValidationSchemaOperation(io.tapstate.spi.store.CachePreparation original) {
        var coordinates = new Document("kind", "initial-validation-schema-v1").append("operationId", original.operationId())
                .append("scope", MongoValidationControlStore.scope(original.businessScope()));
        return java.util.UUID.nameUUIDFromBytes(coordinates.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** CONFIG progress only; CACHE preparation remains an independently verified operation. */
    public record ValidationSchemaStatus(String operationId, boolean ready, long initializedPipelines) {}

    /** Own client and explicit methods; it deliberately exposes no ordinary authoring stores. */
    public static final class ValidationSchemaPreparation implements AutoCloseable {
        private static final com.mongodb.TransactionOptions TRANSACTION = com.mongodb.TransactionOptions.builder()
                .readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
        private final MongoConnection connection;
        private final MongoDatabase database;
        private final String clusterId;
        private final java.util.UUID operation;
        private final MongoStorageBindingStore bindings;
        private final io.tapstate.spi.store.CacheBusinessScope scope;
        private final java.time.Duration timeout;
        private final io.tapstate.spi.store.CachePreparation initial;
        private final Runnable local;
        private final io.tapstate.spi.store.StorageRecoveryCommands.Plan initialRecovery;
        private volatile boolean closed;

        private ValidationSchemaPreparation(MongoConnection connection, String clusterId, java.util.UUID operation, java.time.Duration timeout,
                io.tapstate.spi.store.CachePreparation initial, Runnable local, io.tapstate.spi.store.StorageRecoveryCommands.Plan initialRecovery) {
            this.connection = connection; this.database = connection.database(); this.clusterId = clusterId; this.operation = operation;
            this.bindings = new MongoStorageBindingStore(connection.client(), database, new PersistentKey(database, clusterId));
            this.scope = bindings.cachePreparation().orElseThrow(MongoValidationSchema::refused).businessScope();
            this.timeout = timeout;
            this.initial = initial; this.local = local;
            this.initialRecovery = initialRecovery;
        }

        private static ValidationSchemaPreparation open(MongoConnectionSettings settings, String clusterId, java.util.UUID operation, boolean first) {
            return open(settings, clusterId, operation, first, null, () -> {});
        }
        private static ValidationSchemaPreparation open(MongoConnectionSettings settings, String clusterId, java.util.UUID operation, boolean first,
                io.tapstate.spi.store.CachePreparation initial, Runnable local) {
            return open(settings, clusterId, operation, first, initial, local, null);
        }
        private static ValidationSchemaPreparation open(MongoConnectionSettings settings, String clusterId, java.util.UUID operation, boolean first,
                io.tapstate.spi.store.CachePreparation initial, Runnable local, io.tapstate.spi.store.StorageRecoveryCommands.Plan recovery) {
            Objects.requireNonNull(clusterId); Objects.requireNonNull(operation);
            var connection = MongoConnection.forConfiguration(settings);
            try {
                local.run(); connection.verifyConnectivity(); var handle = new ValidationSchemaPreparation(connection, clusterId, operation, settings.serverSelectionTimeout(), initial, local, recovery);
                if (initial == null) { handle.verifyOffline(); handle.begin(first); }
                else handle.initialConfiguration();
                return handle;
            } catch (RuntimeException | Error failure) { connection.close(); throw failure; }
        }

        private void begin(boolean first) {
            transaction(session -> {
                requireOriginal(session);
                var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(database);
                var row = schemas.find(session, new Document("_id", "config")).first();
                if (!MongoValidationSchema.configurationHasValidation(database, row)) {
                    if (!first) throw MongoValidationSchema.refused();
                    ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), connection.client());
                    MongoValidationSchema.requireAbsent(database, MongoValidationSchema.CONFIG);
                    if (SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,
                            new Document("validationPreparation", new Document("$exists", true)))
                            .projection(new Document("_id", 0).append("found", new Document("$literal", true))).limit(1).first() != null) throw MongoValidationSchema.refused();
                    // The original operation must still be able to settle under schema 1. Publishing
                    // PREPARING first would block both its completion and this marker backfill.
                    if (SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,
                            new Document("phase", new Document("$nin", java.util.List.of("ACTIVE", "RETIRED"))))
                            .projection(new Document("_id", 0).append("found", new Document("$literal", true))).limit(1).first() != null)
                        throw MongoValidationSchema.refused();
                    MongoValidationSchema.create(session, database, MongoValidationSchema.CONFIG);
                    if (schemas.updateOne(session, row, new Document("$set", new Document("installedVersion", MongoValidationSchema.preparedConfigurationVersion(row))
                            .append("validation", MongoValidationSchema.marker(operation.toString(), "PREPARING", 0, scope)))).getMatchedCount() != 1)
                        throw MongoValidationSchema.refused();
                } else requireOperation(row);
                return null;
            });
            MongoValidationSchema.verify(database, MongoValidationSchema.CONFIG);
        }

        private ValidationSchemaStatus initialConfiguration() {
            return transaction(session -> {
                requireOriginal(session);
                var row = SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first();
                if (!MongoValidationSchema.configurationHasValidation(database, row)) {
                    if (SystemCollections.OPERATOR_GENERATIONS.on(database).find(session).projection(new Document("_id", 1)).limit(1).first() != null)
                        throw MongoValidationSchema.refused();
                    MongoValidationSchema.requireAbsent(database, MongoValidationSchema.CONFIG);
                    return null;
                }
                var marker = MongoValidationSchema.requireConfiguration(row, "READY");
                if (!scope.equals(MongoValidationSchema.scope(marker)) || marker.getLong("initializedPipelines") != 0) throw MongoValidationSchema.refused();
                return new ValidationSchemaStatus(marker.getString("operationId"), true, 0);
            });
        }

        private ValidationSchemaStatus prepareInitial(MongoCacheDomain cache) {
            var retained = initialConfiguration();
            cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoValidationSchema::refused));
            if (retained != null) {
                // A completed explicit recovery may have replaced the CACHE schema operation. This
                // path only verifies that published result; it never adopts or creates missing facts.
                MongoValidationSchema.requireReady(database, true);
                cache.verifyValidationReady(initial, retained.operationId());
                cache.verifyBusinessReady(scope); local.run(); return retained;
            }
            cache.resumeValidationSchema(initial, operation.toString());
            try { cache.verifyBusinessReady(scope); }
            catch (TapstateException notReady) {
                // Original controlled creation still arbitrates birth and refuses lost READY facts.
                // A failed read is not the authority to initialize a replacement.
                cache.prepareInitialBusiness(initial, local);
            }
            verifyOffline(); cache.prepareValidationSchema(initial, operation.toString());
            var result = transaction(session -> {
                requireOriginal(session);
                var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(database);
                var row = schemas.find(session, new Document("_id", "config")).first();
                if (!MongoValidationSchema.configurationHasValidation(database, row)) {
                    ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), connection.client());
                    if (SystemCollections.OPERATOR_GENERATIONS.on(database).find(session).projection(new Document("_id", 1)).limit(1).first() != null)
                        throw MongoValidationSchema.refused();
                    MongoValidationSchema.requireAbsent(database, MongoValidationSchema.CONFIG);
                    MongoValidationSchema.create(session, database, MongoValidationSchema.CONFIG);
                    var marker = MongoValidationSchema.marker(operation.toString(), "READY", 0, scope);
                    if (schemas.updateOne(session, row, new Document("$set", new Document("installedVersion", MongoValidationSchema.preparedConfigurationVersion(row)).append("validation", marker))).getMatchedCount() != 1)
                        throw MongoValidationSchema.refused();
                    return status(marker);
                }
                var marker = requireOperation(row);
                if (!"READY".equals(marker.get("phase")) || marker.getLong("initializedPipelines") != 0) throw MongoValidationSchema.refused();
                return status(marker);
            });
            MongoValidationSchema.requireReady(database, true); cache.verifyBusinessReady(scope); local.run(); return result;
        }

        /** Bounded original-operation progress; missing markers cannot be silently counted as unprocessed. */
        public ValidationSchemaStatus advance(int maximumPipelines) {
            if (maximumPipelines < 1 || maximumPipelines > 20) throw new IllegalArgumentException("maintenance batch must contain 1 to 20 Pipelines");
            verifyOffline();
            MongoValidationSchema.verify(database, MongoValidationSchema.CONFIG);
            ValidationSchemaStatus result = null;
            for (int index = 0; index < maximumPipelines; index++) {
                var generation = java.util.UUID.randomUUID();
                result = transaction(session -> {
                    requireOriginal(session); var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(database);
                    var row = schemas.find(session, new Document("_id", "config")).first(); var marker = requireOperation(row);
                    if ("READY".equals(marker.get("phase"))) return status(marker);
                    ConfigurationVersion.advanceValidationPreparation(session, SystemCollections.CONFIGURATION_VERSION.on(database), operation.toString(), connection.client());
                    var mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
                    long initialized = marker.getLong("initializedPipelines");
                    if (mappings.countDocuments(session, new Document("validationPreparation", new Document("$exists", true))) != initialized)
                        throw MongoValidationSchema.unreadable("initialized-pipelines");
                    var missing = new Document("validationPreparation", new Document("$exists", false));
                    var initial = MongoValidationPreparationFacts.encode(new MongoValidationPreparationFacts.State(generation, 0,
                            MongoValidationPreparationFacts.Phase.UNASSIGNED, null, null, null));
                    long changed = mappings.updateOne(session, new Document(missing).append("phase", "ACTIVE"),
                            new Document("$set", new Document("validationPreparation", initial))).getMatchedCount();
                    if (changed == 0) {
                        var retired = MongoValidationPreparationFacts.encode(new MongoValidationPreparationFacts.State(generation, 0,
                                MongoValidationPreparationFacts.Phase.CLOSED, null, operation, null));
                        changed = mappings.updateOne(session, new Document(missing).append("phase", "RETIRED"),
                                new Document("$set", new Document("validationPreparation", retired))).getMatchedCount();
                    }
                    String phase;
                    if (changed == 0) {
                        if (mappings.find(session, missing).projection(new Document("_id", 0).append("found", new Document("$literal", true))).limit(1).first() != null)
                            throw MongoValidationSchema.refused();
                        phase = "READY";
                    } else {
                        if (initialized == Long.MAX_VALUE - 1) throw MongoValidationSchema.unreadable("initialized-pipelines");
                        initialized++; phase = "PREPARING";
                    }
                    var next = MongoValidationSchema.marker(operation.toString(), phase, initialized, scope);
                    if (schemas.updateOne(session, new Document("_id", "config").append("installedVersion", row.getInteger("installedVersion")).append("validation", marker),
                            new Document("$set", new Document("validation", next))).getMatchedCount() != 1) throw MongoValidationSchema.refused();
                    return status(next);
                });
                if (result.ready()) return result;
            }
            return result;
        }

        public void prepareCache(MongoCacheDomain cache) {
            transaction(session -> { requireOriginal(session); requireOperation(SystemCollections.SCHEMA_MIGRATIONS.on(database)
                    .find(session, new Document("_id", "config")).first()); return null; });
            var original = bindings.load().orElseThrow(MongoValidationSchema::refused);
            var expected = bindings.cachePreparation().orElseThrow(MongoValidationSchema::refused);
            cache.requireRecoveryTarget(original);
            cache.prepareValidationSchema(expected, operation.toString());
            transaction(session -> { requireOriginal(session); requireOperation(SystemCollections.SCHEMA_MIGRATIONS.on(database)
                    .find(session, new Document("_id", "config")).first()); return null; });
        }

        private void verifyOffline() {
            boolean complete = transaction(session -> {
                requireOriginal(session); var row = SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first();
                if (!MongoValidationSchema.configurationHasValidation(database, row)) return false;
                return "READY".equals(requireOperation(row).get("phase"));
            });
            var original = bindings.load().orElseThrow(MongoValidationSchema::refused);
            var preparation = bindings.cachePreparation().orElseThrow(MongoValidationSchema::refused);
            try (var cache = MongoCacheDomain.connect(original.cache(), timeout, "validation-schema-maintenance")) {
                cache.requireRecoveryTarget(original);
                cache.verifyValidationMaintenance(preparation, operation.toString(), complete);
            }
        }

        private Document requireOperation(Document row) {
            if (!MongoValidationSchema.configurationHasValidation(database, row)) throw MongoValidationSchema.unreadable("version");
            var marker = MongoValidationSchema.configurationMarker(row);
            if (!operation.toString().equals(marker.get("operationId")) || !scope.equals(MongoValidationSchema.scope(marker))) throw MongoValidationSchema.refused();
            return marker;
        }

        private void requireOriginal(com.mongodb.client.ClientSession session) {
            if (initialRecovery != null) {
                var row = SystemCollections.STORAGE_RECOVERY.on(database).find(session, new Document("_id", "cluster")).first();
                if (row == null || MongoStorageRecoveryCommands.pending(row).filter(initialRecovery::equals).isEmpty()) throw MongoCachePipelineAccess.fenced();
                MongoInitialOpRecovery.requireManifest(connection.client(), session, database, bindings,
                        new MongoConfigurationArtifactStore(connection.client(), database, new PersistentKey(database, clusterId)),
                        initialRecovery.originalScope(), initialRecovery.manifest(), true);
            } else if (initial != null) bindings.requireOriginalInitialPreparation(session, initial);
            var identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
            if (identity == null || !clusterId.equals(identity.get("clusterId")) || !LAYOUT.equals(identity.get("layoutId"))
                    || !bindings.cachePreparationForTransaction(session).map(value -> scope.equals(value.businessScope())).orElse(false)) throw MongoValidationSchema.refused();
        }

        private ValidationSchemaStatus status(Document marker) {
            return new ValidationSchemaStatus(operation.toString(), "READY".equals(marker.get("phase")), marker.getLong("initializedPipelines"));
        }

        private <T> T transaction(java.util.function.Function<com.mongodb.client.ClientSession, T> action) {
            if (closed) throw MongoValidationSchema.refused();
            return StoreIo.call(() -> { try (var session = connection.client().startSession()) {
                local.run();
                var answer = session.withTransaction(() -> {
                    if (closed) throw MongoValidationSchema.refused(); local.run(); var result = action.apply(session); local.run(); return result;
                }, TRANSACTION);
                local.run(); return answer;
            } });
        }

        @Override public void close() { if (!closed) { closed = true; connection.close(); } }
    }

    public io.tapstate.spi.store.AiProviderSettingsStore aiProviderSettings() {
        requireOpen();
        return new MongoConfigurationAiProviderSettingsStore(connection.client(), connection.database(), clusterId,
                new PersistentKey(connection.database(), clusterId), this::requireOpen);
    }

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
        Runnable local = () -> { requireOpen(); active.run(); };
        if (running.phase() == io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING) {
            if (invocation.first && invocation.attempted.compareAndSet(false, true)) cache.prepareAuth(preparation.authScope(), preparation.operationId(), local);
            else cache.resumeInitialAuth(preparation, local);
            original.run(); cache.resumeValidationSchema(preparation, running.operationId());
            cache.prepareConfirmedInitialBusiness(preparation, local); original.run();
            boolean enabled = StoreIo.call(() -> { try (var session = connection.client().startSession()) {
                return session.withTransaction(() -> {
                    requireInitialOpOperation(session, running);
                    return MongoValidationPreparations.validationEnabled(session, connection.database(), clusterId);
                });
            } });
            if (enabled) {
                cache.prepareValidationSchema(preparation, running.operationId()); original.run();
                publishInitialOpValidationSchema(running, local);
            } else {
                try (var schema = ValidationSchemaPreparation.open(connection.configurationSettings(), clusterId, java.util.UUID.fromString(running.operationId()),
                        true, preparation, local, running)) {
                    schema.prepareInitial(cache);
                }
            }
        } else if (running.phase() == io.tapstate.spi.store.StorageRecoveryCommands.Phase.COMPLETE) {
            cache.verifyInitialRecovery(preparation);
            var receipt = initialOpValidationReceipt(running);
            if (receipt != null) {
                MongoValidationSchema.requireReady(connection.database(), true);
                cache.verifyValidationReady(preparation, receipt.operationId());
            }
            if (!Objects.equals(receipt, initialOpValidationReceipt(running))) throw MongoCachePipelineAccess.fenced();
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
                    requireInitialOpOperation(session, original);
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

    private void requireInitialOpOperation(com.mongodb.client.ClientSession session, io.tapstate.spi.store.StorageRecoveryCommands.Plan original) {
        var row = SystemCollections.STORAGE_RECOVERY.on(connection.database()).find(session, new Document("_id", "cluster")).first();
        if (row == null || MongoStorageRecoveryCommands.pending(row).filter(original::equals).isEmpty()) throw MongoCachePipelineAccess.fenced();
        MongoInitialOpRecovery.requireManifest(connection.client(), session, connection.database(), bindings, artifacts,
                original.originalScope(), original.manifest(), true);
    }

    private ValidationSchemaStatus initialOpValidationReceipt(io.tapstate.spi.store.StorageRecoveryCommands.Plan original) {
        return StoreIo.call(() -> { try (var session = connection.client().startSession()) {
            return session.withTransaction(() -> {
                requireInitialOpOperation(session, original);
                var row = SystemCollections.SCHEMA_MIGRATIONS.on(connection.database()).find(session, new Document("_id", "config")).first();
                if (!MongoValidationSchema.configurationHasValidation(connection.database(), row)) return null;
                var marker = MongoValidationSchema.requireConfiguration(row, "READY");
                var scope = new io.tapstate.spi.store.CacheBusinessScope(clusterId, original.originalScope().bindingVersion(), original.originalScope().cacheIncarnation());
                if (!scope.equals(MongoValidationSchema.scope(marker)) || marker.getLong("initializedPipelines") != 0) throw MongoValidationSchema.refused();
                return new ValidationSchemaStatus(marker.getString("operationId"), true, 0);
            });
        } });
    }

    private void publishInitialOpValidationSchema(io.tapstate.spi.store.StorageRecoveryCommands.Plan original, Runnable local) {
        StoreIo.run(() -> { try (var session = connection.client().startSession()) {
            session.withTransaction(() -> {
                requireOpen(); local.run(); requireInitialOpOperation(session, original);
                var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(connection.database());
                var previous = schemas.find(session, new Document("_id", "config")).first();
                var marker = MongoValidationSchema.requireConfiguration(previous, "READY");
                var scope = new io.tapstate.spi.store.CacheBusinessScope(clusterId, original.originalScope().bindingVersion(), original.originalScope().cacheIncarnation());
                if (!scope.equals(MongoValidationSchema.scope(marker)) || marker.getLong("initializedPipelines") != 0) throw MongoValidationSchema.refused();
                if (!original.operationId().equals(marker.get("operationId"))) {
                    ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(connection.database()), connection.client());
                    if (schemas.updateOne(session, previous, new Document("$set", new Document("validation",
                            MongoValidationSchema.marker(original.operationId(), "READY", 0, scope)))).getMatchedCount() != 1) throw MongoValidationSchema.refused();
                }
                requireOpen(); local.run(); return null;
            });
        } });
    }

    /** Prepares only the business domain after atomic all-stopped publication and caller-proven old-writer isolation. */
    public void prepareStoppedRecovery(MongoCacheDomain cache, io.tapstate.spi.store.StorageRecoveryCommands.Plan running,
            Runnable localIsolationGuard) {
        requireOpen();localIsolationGuard.run();
        var recoveries=(MongoCacheRecoveryStore)cacheRecoveries();
        recoveries.requireStopped(running);
        boolean validation = StoreIo.call(() -> {
            try (var session = connection.client().startSession()) {
                return session.withTransaction(() -> {
                    requireOpen(); localIsolationGuard.run(); recoveries.requireStopped(session, running);
                    return MongoValidationPreparations.validationEnabled(session, connection.database(), clusterId);
                });
            }
        });
        if (validation) {
            long started = System.nanoTime();
            while (!recoveries.closeValidationReports(running, 20)) {
                requireOpen(); localIsolationGuard.run();
                if (System.nanoTime() - started >= java.time.Duration.ofSeconds(30).toNanos()) throw MongoValidationSchema.refused();
            }
        }
        cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        // CONFIG validation stays outside CACHE transactions; their callbacks only check this local lifecycle.
        if (validation) cache.resumeValidationSchema(new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope()), running.operationId());
        cache.prepareConfirmedInitialBusiness(new io.tapstate.spi.store.CachePreparation(running.operationId(),running.originalScope()),
                () -> {requireOpen();localIsolationGuard.run();});
        if (validation) {
            cache.prepareValidationSchema(new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope()), running.operationId());
            recoveries.requireStopped(running); localIsolationGuard.run();
            publishValidationRecoverySchema(running, localIsolationGuard, session -> recoveries.requireStopped(session, running));
        }
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
                        requireInitialCloudRecovery(session, running);
                        return null;
                    });
                }
            });
        };
        original.run(); cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        boolean validation = StoreIo.call(() -> {
            try (var session = connection.client().startSession()) {
                return session.withTransaction(() -> {
                    requireOpen(); requireActive.run(); requireInitialCloudRecovery(session, running);
                    boolean enabled = MongoValidationPreparations.validationEnabled(session, connection.database(), clusterId);
                    if (enabled && SystemCollections.VALIDATION_TASKS.on(connection.database()).find(session,
                            MongoValidationPreparations.unfinishedReports(clusterId, null)).limit(1).first() != null) throw MongoValidationSchema.refused();
                    return enabled;
                });
            }
        });
        if (validation) cache.resumeValidationSchema(new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope()), running.operationId());
        cache.prepareConfirmedInitialBusiness(new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope()), original);
        if (validation) {
            cache.prepareValidationSchema(new io.tapstate.spi.store.CachePreparation(running.operationId(), running.originalScope()), running.operationId());
            original.run();
            publishValidationRecoverySchema(running, requireActive, session -> requireInitialCloudRecovery(session, running));
        }
        original.run();
    }

    private void requireInitialCloudRecovery(com.mongodb.client.ClientSession session, io.tapstate.spi.store.StorageRecoveryCommands.Plan running) {
        var root = SystemCollections.STORAGE_RECOVERY.on(connection.database()).find(session, new Document("_id", "cluster")).first();
        if (root == null || !MongoStorageRecoveryCommands.pending(root).filter(running::equals).isPresent()
                || running.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING) throw MongoCachePipelineAccess.fenced();
        MongoInitialCloudRecovery.requireManifest(session, connection.database(), bindings, artifacts, running.originalScope(), running.manifest());
    }

    private void publishValidationRecoverySchema(io.tapstate.spi.store.StorageRecoveryCommands.Plan running, Runnable local,
            java.util.function.Consumer<com.mongodb.client.ClientSession> requireOriginal) {
        StoreIo.run(() -> {
            try (var session = connection.client().startSession()) {
                session.withTransaction(() -> {
                    requireOpen(); local.run(); requireOriginal.accept(session);
                    if (running.manifest().affectedPipelineIds().isEmpty() && SystemCollections.VALIDATION_TASKS.on(connection.database()).find(session,
                            MongoValidationPreparations.unfinishedReports(clusterId, null)).limit(1).first() != null) throw MongoValidationSchema.refused();
                    for (String pipeline : running.manifest().affectedPipelineIds()) {
                        MongoValidationPreparations.requireCacheReplacement(session, connection.database(), clusterId, pipeline, running);
                        MongoValidationAdmissionStore.requireClosedPipelinePermissions(session, connection.database(), clusterId, pipeline);
                    }
                    var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(connection.database());
                    var original = schemas.find(session, new Document("_id", "config")).first();
                    var marker = MongoValidationSchema.requireConfiguration(original, "READY");
                    var scope = new io.tapstate.spi.store.CacheBusinessScope(clusterId, running.originalScope().bindingVersion(), running.originalScope().cacheIncarnation());
                    if (scope.equals(MongoValidationSchema.scope(marker)) && !running.operationId().equals(marker.get("operationId"))) throw MongoValidationSchema.refused();
                    if (schemas.updateOne(session, original, new Document("$set", new Document("validation",
                            MongoValidationSchema.marker(running.operationId(), "READY", marker.getLong("initializedPipelines"), scope)))).getMatchedCount() != 1)
                        throw MongoValidationSchema.refused();
                    requireOpen(); local.run(); return null;
                });
            }
        });
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

    synchronized MongoValidationPreparations validationPreparations() {
        requireOpen();
        StoreIo.run(() -> MongoValidationSchema.requireReady(connection.database(), true));
        if (validationPreparations == null) validationPreparations = new MongoValidationPreparations(connection.client(), connection.database(), clusterId, bindings, artifacts, this::requireOpen);
        return validationPreparations;
    }

    MongoValidationAdmissionStore validationAdmissions(MongoValidationPreparations.Permission original) {
        requireOpen(); return new MongoValidationAdmissionStore(connection.client(), connection.database(), clusterId,
                this::requireOpen, new MongoValidationPreparations.Authority(original));
    }

    /** Configuration authoring remains CONFIG-only and never requires a running validation owner. */
    public io.tapstate.spi.store.ValidationTaskStore validationTasks(Runnable local) {
        requireOpen(); Objects.requireNonNull(local).run();
        StoreIo.run(() -> MongoValidationSchema.requireReady(connection.database(), true));
        return MongoValidationAdmissionStore.taskDefinitions(connection.client(), connection.database(), clusterId,
                () -> { requireOpen(); local.run(); }, artifacts);
    }

    /** Retained CONFIG facts only; reading history neither creates a native preparation nor acquires an owner. */
    public io.tapstate.spi.store.ValidationReports validationReports(Runnable local){
        requireOpen();Objects.requireNonNull(local).run();StoreIo.run(()->MongoValidationSchema.requireReady(connection.database(),true));
        return new MongoValidationResultStore(connection.client(),connection.database(),clusterId,()->{requireOpen();local.run();});
    }

    /** Independent preparation only. The caller authenticates its command and supplies a pure local lifecycle guard. */
    public java.util.UUID currentValidationGeneration(String pipeline, io.tapstate.spi.store.CacheBusinessScope expected, Runnable local) {
        requireOpen(); Objects.requireNonNull(local).run();
        return validationPreparations().guardedBy(local).currentGeneration(pipeline, Objects.requireNonNull(expected));
    }

    /** Independent preparation only. The caller authenticates its command and supplies a pure local lifecycle guard. */
    public ValidationAccess prepareValidationAccess(String pipeline, io.tapstate.spi.store.CacheBusinessScope expected,
            MongoCacheDomain cache, io.tapstate.spi.store.WorkloadClaimFence node, Runnable local) {
        Objects.requireNonNull(local).run(); requireOpen(); Objects.requireNonNull(expected); Objects.requireNonNull(cache);
        if (node == null || node.key().type() != io.tapstate.spi.store.WorkloadClaimType.NODE_SESSION
                || !expected.clusterId().equals(node.key().clusterId())) throw MongoValidationPreparations.fenced();
        cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        var cachePreparation = cache.validationPreparations(expected, local);
        var configuration = validationPreparations().guardedBy(local); var original = configuration.issue(pipeline, expected);
        var completed = configuration.complete(original, cachePreparation.prepare(original, node));
        local.run(); requireOpen(); return new ValidationAccess(this, cache, completed, local);
    }
    /** Read-only access to an already completed original. Missing preparation is never initialized. */
    public ValidationAccess loadValidationAccess(String pipeline,io.tapstate.spi.store.CacheBusinessScope expected,MongoCacheDomain cache,Runnable local){
        Objects.requireNonNull(local).run();requireOpen();Objects.requireNonNull(expected);Objects.requireNonNull(cache);
        cache.requireRecoveryTarget(bindings.load().orElseThrow(MongoCachePipelineAccess::fenced));
        var original=validationPreparations().guardedBy(local).loadComplete(pipeline,expected);
        local.run();requireOpen();return new ValidationAccess(this,cache,original,local);
    }

    /** Fixed native preparation, not a renewable owner grant. Each caller still proves its exact owner/run. */
    public static final class ValidationAccess {
        private final MongoConfigurationDomain configuration;
        private final MongoCacheDomain cache;
        private final MongoValidationPreparations.Permission original;
        private final Runnable local;
        private ValidationAccess(MongoConfigurationDomain configuration, MongoCacheDomain cache,
                MongoValidationPreparations.Permission original, Runnable local) {
            this.configuration = configuration; this.cache = cache; this.original = original; this.local = local;
        }
        public String pipelineId() { return original.subject().pipelineId(); }
        public io.tapstate.spi.store.CacheBusinessScope scope() { return original.subject().scope(); }
        public java.util.UUID generation() { return original.subject().generation(); }
        public java.util.UUID preparationId() { return original.subject().preparationId(); }
        public void requireCurrent() { configuration.requireOpen(); local.run(); original.requireComplete(); }
        public io.tapstate.spi.store.WorkloadClaimStore claims() { requireCurrent(); return cache.validationClaims(original, local); }
        public io.tapstate.spi.store.ValidationControlStore control() { requireCurrent(); return cache.validationControl(original, local); }
        public io.tapstate.spi.store.ValidationRunHistory history(){requireCurrent();return cache.validationHistory(original,local);}
        public io.tapstate.spi.store.ValidationReportAdmission admissions() {
            requireCurrent(); return new MongoValidationAdmissionStore(configuration.connection.client(), configuration.connection.database(), configuration.clusterId,
                    () -> { configuration.requireOpen(); local.run(); }, new MongoValidationPreparations.Authority(original));
        }
        public io.tapstate.spi.store.ValidationResultStore results() {
            requireCurrent(); return new MongoValidationResultStore(configuration.connection.client(), configuration.connection.database(), configuration.clusterId,
                    () -> { configuration.requireOpen(); local.run(); }, new MongoValidationPreparations.Authority(original));
        }
        public io.tapstate.spi.store.ValidationReports reports(){
            requireCurrent();return new MongoValidationResultStore(configuration.connection.client(),configuration.connection.database(),configuration.clusterId,
                    ()->{configuration.requireOpen();local.run();},new MongoValidationPreparations.Authority(original));
        }
        /** Only old-tenure CONFIG rights are revoked; current requests and saved task definitions remain intact. */
        public boolean closeObsoletePermissions(io.tapstate.core.validation.ValidationOwner current, int maximum) {
            requireCurrent(); var control = control(); control.snapshot(Objects.requireNonNull(current));
            var admissions = new MongoValidationAdmissionStore(configuration.connection.client(), configuration.connection.database(), configuration.clusterId,
                    () -> { configuration.requireOpen(); local.run(); }, new MongoValidationPreparations.Authority(original));
            boolean complete = admissions.transaction(session -> admissions.closeObsoletePermissionsInTransaction(session, current, maximum));
            control.snapshot(current); requireCurrent(); return complete;
        }
    }

    /** Bounded metadata retirement for an original native deletion. No production preparation or business IO. */
    public boolean retireValidation(io.tapstate.spi.store.PipelineRetirementPreparation original, MongoCacheDomain cache,
            io.tapstate.spi.store.WorkloadClaimFence node, Runnable local) {
        Objects.requireNonNull(local).run(); requireOpen(); MongoPipelineRetirements.requireOriginalPending(original);
        boolean enabled = StoreIo.call(() -> {
            var schema = SystemCollections.SCHEMA_MIGRATIONS.on(connection.database()).find(new Document("_id", "config")).first();
            if (!MongoValidationSchema.configurationHasValidation(connection.database(), schema)) return false;
            MongoValidationSchema.requireReady(connection.database(), true); return true;
        });
        if (!enabled) return true;
        var preparation = validationPreparations().guardedBy(local); var closing = preparation.forRetirement(original);
        if (!preparation.closeReports(closing, 20)) return false;
        preparation.requireReportsClosed(closing); var state = closing.requireCurrent();
        if (state.phase() == MongoValidationPreparationFacts.Phase.CLOSED) return true;
        if (state.subject() == null) { preparation.finishClose(closing, null); return true; }
        var controls = cache.validationPreparations(state.subject().scope(), local);
        var result = controls.cancel(closing, node);
        if (result.requireResult(state.subject()).outcome() == MongoValidationPreparationFacts.Outcome.PREPARED) {
            var cleanup = controls.cleanupOwner(closing, node, java.time.Duration.ofSeconds(30));
            var authority = io.tapstate.spi.store.ValidationClaims.owner(state.subject().pipelineId(), cleanup);
            var current = cache.validationControl(closing, local);
            var reports = new MongoValidationResultStore(connection.client(), connection.database(), clusterId,
                    () -> { requireOpen(); local.run(); }, new MongoValidationPreparations.Authority(closing));
            for (var run : controls.activeRuns(closing, 20)) {
                local.run(); reports.seal(run.report());
                current.terminateLostOwner(new io.tapstate.spi.store.ValidationControlStore.LostOwner(authority, run, run.report()));
            }
            result = controls.cancel(closing, node);
            if (result.requireResult(state.subject()).outcome() == MongoValidationPreparationFacts.Outcome.PREPARED) return false;
        }
        preparation.finishClose(closing, result); local.run(); return true;
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
