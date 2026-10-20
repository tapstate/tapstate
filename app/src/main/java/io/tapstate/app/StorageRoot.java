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
    private final StorageRuntime runtime;
    private final StorageAuthentication authentication;
    private final Optional<StorageBinding.Connection> endpoint;
    private volatile Phase phase;
    private volatile boolean closed;

    StorageRoot(MongoProperties mongo, CloudRuntimeSettings cloud, CloudSdkBridge bridge, BootId boot,
            MetricsHistoryProperties history, TokenSecrets secrets, Clock clock, byte[] signingKey) {
        this.cloud = Objects.requireNonNull(cloud);
        String uri = cloud.metadataUri(mongo.getUri());
        StorageBinding.Connection selectedEndpoint = null;
        MongoConfigurationDomain configuration = null;
        phase = Phase.CONFIGURATION_REQUIRED;
        if (mongo.isEnabled() && uri != null && !uri.isBlank()) {
            try {
                selectedEndpoint = new StorageBinding.Connection(
                        cloud.cloud() ? StorageBinding.Connection.Kind.ATLAS : StorageBinding.Connection.Kind.MONGODB,
                        uri, MongoDatabaseNames.fromUri(uri), readCa(mongo.getTlsCaFile()));
                var settings = new MongoConnectionSettings(uri, mongo.getTlsCaFile(), mongo.getServerSelectionTimeout(), boot.value());
                configuration = cloud.cloud() ? MongoConfigurationDomain.open(settings, cloud.clusterId())
                        : MongoConfigurationDomain.openExisting(settings);
                phase = Phase.SETUP_REQUIRED;
            } catch (TapstateException unavailable) {
                phase = Phase.CONFIGURATION_UNAVAILABLE;
            }
        }
        endpoint = configuration == null ? Optional.empty() : Optional.of(selectedEndpoint);
        MongoConfigurationDomain original = configuration;
        this.runtime = new StorageRuntime(configuration, binding -> {
            if (original == null) throw StorageRuntime.unavailable();
            var preparation = original.bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable);
            var target = original.artifacts().get(binding.defaultSourceId()).filter(SourceResource.class::isInstance)
                    .map(SourceResource.class::cast).orElseThrow(StorageRuntime::unavailable);
            if (!(target.config().get("uri") instanceof String masterUri)) throw StorageRuntime.unavailable();
            var databases = new OperatorDatabaseBinding(preparation.businessScope(), binding.cache().database(),
                    original.databaseName(), MongoDatabaseNames.fromUri(masterUri));
            return MongoCacheDomain.connect(binding.cache(), mongo.getServerSelectionTimeout(), boot.value(), history.getRetention(), databases);
        });
        this.authentication = new StorageAuthentication(configuration, cloud, bridge, secrets, clock, signingKey);
    }

    Phase phase() { requireOpen(); return phase; }
    StorageRuntime runtime() { requireOpen(); return runtime; }
    StorageAuthentication authentication() { requireOpen(); return authentication; }
    Optional<StorageBinding.Connection> configurationEndpoint() { requireOpen(); return endpoint; }
    void requireActive() { requireOpen(); }
    StorageBinding.Mode mode() { return cloud.cloud() ? StorageBinding.Mode.CLOUD : StorageBinding.Mode.OP; }

    /** Explicit startup/operation continuation only. Status reads never connect or initialize a cache. */
    synchronized void loadExisting(Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> assembly) {
        requireOpen();
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
        try { return authentication.local(); }
        catch (TapstateException unavailable) {
            if (unavailable.code() != ControlError.STORAGE_UNAVAILABLE) throw unavailable;
            restoreAuthentication(); return authentication.local();
        }
    }

    synchronized ManagedAuthenticationContext managedAuthentication() {
        requireOpen();
        try { return authentication.managed(); }
        catch (TapstateException unavailable) {
            if (unavailable.code() != ControlError.STORAGE_UNAVAILABLE) throw unavailable;
            restoreAuthentication(); return authentication.managed();
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
        boolean awaitingRebuild = !admitted && preparation.map(value -> value.authScope().authGeneration() > 1).orElse(false)
                && phase == Phase.AUTH_READY;
        if (awaitingRebuild) business = StorageControlDispatcher.BusinessPhase.AWAITING_REBUILD_CONFIRMATION;
        active.run();
        return new StorageControlDispatcher.Snapshot(mode(), status.scope().clusterId(), awaitingRebuild
                ? preparation.orElseThrow().operationId() : status.scope().operationId(),
                status.scope().generation(), status.bindingVersion(), preparation.map(value -> value.authScope().authGeneration()).orElse(0L),
                configuration.configurationRevision(), status.phase().name(), business, health,
                status.phase() == StorageSetupStateStore.Phase.CONFIG_PREPARED && mode() == StorageBinding.Mode.CLOUD
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
        authentication.close(); runtime.close();
    }
}
