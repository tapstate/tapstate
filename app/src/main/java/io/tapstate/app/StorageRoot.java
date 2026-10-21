package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.*;
import java.util.function.Function;

/** CONFIG-first production ownership. Reads cannot turn an unavailable existing installation into a new one. */
final class StorageRoot implements AutoCloseable {
    enum Phase { CONFIGURATION_REQUIRED, CONFIGURATION_UNAVAILABLE, SETUP_REQUIRED, CACHE_UNAVAILABLE, AUTH_READY, BUSINESS_READY }
    private final CloudRuntimeSettings cloud;
    private volatile StorageRuntime runtime;
    private volatile StorageAuthentication authentication;
    private final Optional<StorageBinding.Connection> endpoint;
    private final java.util.function.Supplier<MongoConfigurationDomain> configurationLoader;
    private final Function<MongoConfigurationDomain, StorageRuntime> runtimes;
    private final Function<MongoConfigurationDomain, StorageAuthentication> authentications;
    private final CloudConfigurationInitialization initialization;
    private final byte[] authenticationKey;
    private volatile Phase phase;
    private volatile boolean closed;
    private FirstCacheCreation attemptedCreation;
    private Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> retainedAssembly;

    StorageRoot(MongoProperties mongo, CloudRuntimeSettings cloud, CloudSdkBridge bridge, BootId boot,
            MetricsHistoryProperties history, TokenSecrets secrets, Clock clock, byte[] signingKey) {
        this.cloud = Objects.requireNonNull(cloud);
        String uri = cloud.metadataUri(mongo.getUri());
        StorageBinding.Connection selectedEndpoint = null;
        MongoConnectionSettings selectedSettings = null;
        phase = Phase.CONFIGURATION_REQUIRED;
        if (mongo.isEnabled() && uri != null && !uri.isBlank()) {
            try {
                selectedEndpoint = new StorageBinding.Connection(
                        // Platform CONFIG retains its supplied driver/authentication policy. Only the
                        // customer's CACHE and MASTER inputs use the restricted Atlas setup policy.
                        StorageBinding.Connection.Kind.MONGODB,
                        uri, MongoDatabaseNames.fromUri(uri), readCa(mongo.getTlsCaFile()));
                selectedSettings = new MongoConnectionSettings(uri, mongo.getTlsCaFile(), mongo.getServerSelectionTimeout(), boot.value());
            } catch (TapstateException unavailable) {
                phase = Phase.CONFIGURATION_UNAVAILABLE;
            }
        }
        endpoint = Optional.ofNullable(selectedEndpoint);
        var settings = selectedSettings;
        initialization = settings != null && cloud.cloud() ? new CloudConfigurationInitialization(settings, cloud, bridge, boot) : null;
        configurationLoader = settings == null ? null : initialization != null ? initialization::open : () -> MongoConfigurationDomain.openExisting(settings);
        authenticationKey = signingKey.clone();
        authentications = configuration -> new StorageAuthentication(configuration, cloud, bridge, secrets, clock, authenticationKey);
        runtimes = original -> new StorageRuntime(original, binding -> {
            if (original == null) throw StorageRuntime.unavailable();
            var preparation = original.bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable);
            var target = original.artifacts().get(binding.defaultSourceId()).filter(SourceResource.class::isInstance)
                    .map(SourceResource.class::cast).orElseThrow(StorageRuntime::unavailable);
            if (!(target.config().get("uri") instanceof String masterUri)) throw StorageRuntime.unavailable();
            var databases = new OperatorDatabaseBinding(preparation.businessScope(), binding.cache().database(),
                    original.databaseName(), MongoDatabaseNames.fromUri(masterUri));
            return MongoCacheDomain.connect(binding.cache(), mongo.getServerSelectionTimeout(), boot.value(), history.getRetention(), databases);
        });
        runtime = runtimes.apply(null); authentication = authentications.apply(null);
        loadConfiguration();
    }

    /** Explicit Cloud entry can retry initial CONFIG IO using the same process-owned creation invocation. */
    private synchronized void loadConfiguration() {
        requireOpen();
        if (configurationLoader == null || runtime.configuration().isPresent()) return;
        MongoConfigurationDomain candidate = null;
        try {
            candidate = configurationLoader.get();
            var nextRuntime = runtimes.apply(candidate);
            var nextAuthentication = authentications.apply(candidate);
            authentication.close(); runtime.close();
            runtime = nextRuntime; authentication = nextAuthentication; phase = Phase.SETUP_REQUIRED;
        } catch (RuntimeException | Error failure) {
            if (candidate != null) try { candidate.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            if (!(failure instanceof TapstateException)) throw failure;
            phase = Phase.CONFIGURATION_UNAVAILABLE;
        }
    }

    Phase phase() { requireOpen(); return phase; }
    StorageRuntime runtime() { requireOpen(); return runtime; }
    StorageAuthentication authentication() { requireOpen(); return authentication; }
    Optional<StorageBinding.Connection> configurationEndpoint() {
        requireOpen(); return runtime.configuration().isPresent() ? endpoint : Optional.empty();
    }
    void requireActive() { requireOpen(); }
    StorageBinding.Mode mode() { return cloud.cloud() ? StorageBinding.Mode.CLOUD : StorageBinding.Mode.OP; }

    /** Continues only the original committed setup. No request owns or closes the retained CACHE client. */
    synchronized void continueCloudSetup(StorageBindingCommit committed, StorageSetupService.DefaultTarget target,
            Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> assembly) {
        requireOpen();
        if (!cloud.cloud() || committed.binding().mode() != StorageBinding.Mode.CLOUD || runtime.cleanupRequired())
            throw StorageRuntime.unavailable();
        var original = runtime.loadBound().orElseThrow(StorageRuntime::unavailable);
        Runnable active = () -> {
            requireOpen(); original.requireActive();
            if (runtime.current().orElseThrow(StorageRuntime::unavailable) != original
                    || !original.binding().equals(committed.binding()) || !original.preparation().equals(committed.preparation())
                    || !runtime.requireConfiguration().bindings().load().orElseThrow(StorageRuntime::unavailable).equals(committed.binding())
                    || !runtime.requireConfiguration().bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable).equals(committed.preparation()))
                throw StorageRuntime.unavailable();
        };
        active.run();
        var configuration = runtime.requireConfiguration();
        if (configuration.setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE) {
            phase = Phase.SETUP_REQUIRED;
            // Reload the saved encrypted definition and its trust material; a probe's live client is never adopted.
            var source = configuration.artifacts().get(committed.binding().defaultSourceId())
                    .filter(SourceResource.class::isInstance).map(SourceResource.class::cast).orElseThrow(StorageRuntime::unavailable);
            if (!source.connector().equals("mongodb-atlas") || !(source.config().get("uri") instanceof String uri))
                throw StorageRuntime.unavailable();
            var master = new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS, uri, MongoDatabaseNames.fromUri(uri),
                    source.config().get(StorageSetupService.CA_BUNDLE_FIELD) instanceof String ca ? ca : null);
            target.test(master, source, target.identity(master)); active.run();
            if (committed.firstCreation().isPresent() && attemptedCreation != committed.firstCreation().orElseThrow()) {
                attemptedCreation = committed.firstCreation().orElseThrow();
                original.cache().prepareFirstAuth(attemptedCreation);
            } else original.cache().resumeInitialAuth(committed.preparation(), active);
            original.verifyAuthentication();
            original.cache().prepareInitialBusiness(committed.preparation(), active);
            original.verifyBusiness(); active.run();
            configuration.bindings().guardedBy(active).complete(committed.binding().setup());
        }
        active.run();
        authentication.install(original); phase = Phase.AUTH_READY;
        runtime.startBusiness(original, assembly); phase = Phase.BUSINESS_READY;
    }

    /** Explicit startup/operation continuation only. Status reads never connect or initialize a cache. */
    synchronized void loadExisting(Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> assembly) {
        requireOpen();
        retainedAssembly = Objects.requireNonNull(assembly);
        if (runtime.configuration().isEmpty()) return;
        try {
            var original = runtime.loadBound();
            if (original.isEmpty()) { phase = Phase.SETUP_REQUIRED; return; }
            if (cloud.cloud() && runtime.requireConfiguration().setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE) {
                phase = Phase.SETUP_REQUIRED; return;
            }
            phase = Phase.CACHE_UNAVAILABLE;
            try { authentication.install(original.orElseThrow()); }
            catch (TapstateException unavailable) { restoreAuthentication(); }
            original = runtime.current(); phase = Phase.AUTH_READY;
            if (runtime.requireConfiguration().setupStates().load().phase() == StorageSetupStateStore.Phase.COMPLETE) {
                runtime.startBusiness(original.orElseThrow(), assembly); phase = Phase.BUSINESS_READY;
            }
        } catch (TapstateException unavailable) {
            if (phase != Phase.AUTH_READY) phase = Phase.CACHE_UNAVAILABLE;
        }
    }

    synchronized LocalAuthenticationContext localAuthentication() {
        requireOpen();
        if (runtime.configuration().isEmpty()) throw StorageRuntime.unavailable();
        try { return authentication.local(); }
        catch (TapstateException unavailable) {
            if (unavailable.code() != ControlError.STORAGE_UNAVAILABLE) throw unavailable;
            restoreAuthentication(); return authentication.local();
        }
    }

    synchronized ManagedAuthenticationContext managedAuthentication() {
        requireOpen();
        if (runtime.configuration().isEmpty()) loadConfiguration();
        if (runtime.configuration().isEmpty()) throw StorageRuntime.unavailable();
        ManagedAuthenticationContext context;
        try { context = authentication.managed(); }
        catch (TapstateException unavailable) {
            if (unavailable.code() != ControlError.STORAGE_UNAVAILABLE) throw unavailable;
            restoreAuthentication(); context = authentication.managed();
        }
        resumeRetainedBusiness(); return context;
    }

    private void resumeRetainedBusiness() {
        if (retainedAssembly == null || runtime.cleanupRequired() || runtime.business().isPresent()) return;
        if (runtime.requireConfiguration().setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE) return;
        try {
            var original = runtime.current().orElseThrow(StorageRuntime::unavailable);
            runtime.startBusiness(original, retainedAssembly);
            phase = Phase.BUSINESS_READY;
        } catch (TapstateException unavailable) {
            // Formal login remains usable while a missing or failed business domain stays gated.
            phase = Phase.AUTH_READY;
        }
    }

    private void restoreAuthentication() {
        phase = Phase.CACHE_UNAVAILABLE;
        authentication.invalidateFormal();
        try {
            var restored = runtime.restoreAuthentication();
            authentication.install(restored); phase = runtime.businessStarted() ? Phase.BUSINESS_READY : Phase.AUTH_READY;
        } catch (TapstateException unavailable) {
            throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), unavailable);
        }
    }

    StorageControlDispatcher.Snapshot snapshot(StorageControlAccess caller, Runnable active) {
        requireOpen(); active.run();
        var configuration = runtime.requireConfiguration(); var status = configuration.setupStates().guardedBy(active).load();
        var preparation = configuration.bindings().guardedBy(active).cachePreparation();
        boolean admitted = phase == Phase.BUSINESS_READY && runtime.businessStarted();
        var health = new EnumMap<StorageSetupService.Layer,StorageControlDispatcher.Health>(StorageSetupService.Layer.class);
        health.put(StorageSetupService.Layer.CONFIG, StorageControlDispatcher.Health.AVAILABLE);
        health.put(StorageSetupService.Layer.CACHE, switch (phase) {
            case BUSINESS_READY -> admitted ? StorageControlDispatcher.Health.AVAILABLE : StorageControlDispatcher.Health.AUTH_READY;
            case AUTH_READY -> StorageControlDispatcher.Health.AUTH_READY;
            case CACHE_UNAVAILABLE -> StorageControlDispatcher.Health.UNAVAILABLE;
            default -> StorageControlDispatcher.Health.UNKNOWN;
        });
        health.put(StorageSetupService.Layer.MASTER, StorageControlDispatcher.Health.UNKNOWN);
        var business = switch (phase) {
            case BUSINESS_READY -> admitted ? StorageControlDispatcher.BusinessPhase.READY : StorageControlDispatcher.BusinessPhase.AUTH_READY;
            case AUTH_READY -> StorageControlDispatcher.BusinessPhase.AUTH_READY;
            case SETUP_REQUIRED -> StorageControlDispatcher.BusinessPhase.NOT_CONFIGURED;
            default -> StorageControlDispatcher.BusinessPhase.UNAVAILABLE;
        };
        if (status.phase() == StorageSetupStateStore.Phase.STORAGE_BOUND) business = StorageControlDispatcher.BusinessPhase.PREPARING;
        if (runtime.cleanupRequired()) business = StorageControlDispatcher.BusinessPhase.CLEANUP_REQUIRED;
        boolean awaitingRebuild = !admitted && preparation.map(value -> value.authScope().authGeneration() > 1).orElse(false)
                && phase == Phase.AUTH_READY;
        if (awaitingRebuild) business = StorageControlDispatcher.BusinessPhase.AWAITING_REBUILD_CONFIRMATION;
        active.run();
        return new StorageControlDispatcher.Snapshot(mode(), status.scope().clusterId(), awaitingRebuild
                ? preparation.orElseThrow().operationId() : status.scope().operationId(),
                status.scope().generation(), status.bindingVersion(), preparation.map(value -> value.authScope().authGeneration()).orElse(0L),
                configuration.configurationRevision(), status.phase().name(), business, health,
                status.phase() != StorageSetupStateStore.Phase.COMPLETE && mode() == StorageBinding.Mode.CLOUD && !runtime.cleanupRequired()
                        ? List.of("storage.status", "storage.setup.status", "storage.test", "storage.setup.commit")
                        : List.of("storage.status", "storage.setup.status"), false);
    }

    private void requireOpen() { if (closed) throw StorageRuntime.unavailable(); }
    private static String readCa(String path) {
        if (path == null || path.isBlank()) return null;
        try { return java.nio.file.Files.readString(java.nio.file.Path.of(path)); }
        catch (java.io.IOException invalid) { throw StorageRuntime.unavailable(); }
    }
    @Override public synchronized void close() {
        if (closed) return; closed = true;
        if (initialization != null) initialization.close();
        try { authentication.close(); runtime.close(); }
        finally { java.util.Arrays.fill(authenticationKey, (byte)0); }
    }
}
