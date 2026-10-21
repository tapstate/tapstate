package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CachePreparation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Owns CONFIG independently from one original cache connection and its admitted runtime resources. */
final class StorageRuntime implements AutoCloseable {
    interface Business extends AutoCloseable {
        /** Resources are registered with this owner before startup creates members or background work. */
        void start();
        default boolean available() { return true; }
        /** A candidate may wait for its existing membership gate; it must never override that gate. */
        default void awaitAvailability() {}
        @Override void close();
    }
    private final MongoConfigurationDomain configuration;
    private final Function<StorageBinding, MongoCacheDomain> connections;
    private Bound bound;
    private AutoCloseable business;
    private volatile boolean closed;
    private boolean cleanupUnknown;
    private final Runnable ownerActive;

    StorageRuntime(MongoConfigurationDomain configuration, Function<StorageBinding, MongoCacheDomain> connections) {
        this(configuration, connections, () -> {});
    }
    StorageRuntime(MongoConfigurationDomain configuration, Function<StorageBinding, MongoCacheDomain> connections, Runnable ownerActive) {
        this.configuration = configuration;
        this.connections = Objects.requireNonNull(connections, "connections");
        this.ownerActive = Objects.requireNonNull(ownerActive, "ownerActive");
    }

    synchronized Optional<MongoConfigurationDomain> configuration() {
        requireOpen(); return Optional.ofNullable(configuration);
    }

    synchronized MongoConfigurationDomain requireConfiguration() {
        return configuration().orElseThrow(StorageRuntime::unavailable);
    }

    /** An ordinary restart only loads retained identities; it never creates auth or business facts. */
    synchronized Optional<Bound> loadBound() {
        requireOpen();
        if (configuration == null) return Optional.empty();
        var binding = configuration.bindings().load();
        if (binding.isEmpty()) {
            if (bound != null) throw unavailable();
            return Optional.empty();
        }
        var preparation = configuration.bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable);
        if (bound != null) {
            if (!bound.binding.equals(binding.orElseThrow()) || !bound.preparation.equals(preparation)) throw unavailable();
            bound.requireActive(); return Optional.of(bound);
        }
        if (cleanupUnknown) throw unavailable();
        MongoCacheDomain cache = connections.apply(binding.orElseThrow());
        try {
            Bound candidate = new Bound(binding.orElseThrow(), preparation, cache);
            bound = candidate;
            return Optional.of(candidate);
        } catch (RuntimeException | Error failure) {
            try { cache.close(); } catch (RuntimeException | Error cleanup) {
                cleanupUnknown = true;
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    synchronized Optional<Bound> current() { requireOpen(); return Optional.ofNullable(bound); }
    synchronized boolean cleanupRequired() { return cleanupUnknown; }

    /** Retires this process's original owners before loading a newly published authentication scope. */
    synchronized Bound restoreAuthentication() {
        requireOpen();
        if (configuration == null || cleanupUnknown) throw unavailable();
        var binding = configuration.bindings().load().orElseThrow(StorageRuntime::unavailable);
        MongoCacheDomain candidate = connections.apply(binding);
        Throwable failure = null;
        try {
            boolean recovery = configuration.authenticationRecoveryRequired(candidate, this::requireOpen);
            var preparation = configuration.bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable);
            if (recovery || bound != null && (!bound.binding.equals(binding) || !bound.preparation.equals(preparation))) retireBound();
            if (recovery) configuration.prepareAuthenticationRecovery(candidate, this::requireOpen);
        } catch (RuntimeException | Error stopped) {
            failure = stopped;
            throw stopped;
        } finally {
            try { candidate.close(); } catch (RuntimeException | Error cleanup) {
                cleanupUnknown = true;
                if (failure != null) { if (failure != cleanup) failure.addSuppressed(cleanup); }
                else throw cleanup;
            }
        }
        return loadBound().orElseThrow(StorageRuntime::unavailable);
    }

    private void retireBound() {
        Bound original = bound;
        AutoCloseable owned = business;
        bound = null; business = null;
        if (original != null) original.active = false;
        Throwable failure = null;
        if (owned != null) {
            try { owned.close(); } catch (Exception | Error cleanup) { failure = cleanup; }
        }
        if (original != null) {
            try { original.cache.close(); } catch (RuntimeException | Error cleanup) {
                if (failure == null) failure = cleanup; else if (failure != cleanup) failure.addSuppressed(cleanup);
            }
        }
        if (failure != null) {
            cleanupUnknown = true;
            if (failure instanceof Error error) throw error;
            var unavailable = unavailable(); unavailable.addSuppressed(failure); throw unavailable;
        }
    }

    /** The caller passes the original owner, so a late callback cannot activate or close its successor. */
    synchronized void startBusiness(Bound original, Function<Bound, ? extends Business> assembly) {
        startRecoveredBusiness(original, assembly, null, () -> {});
    }

    synchronized void startRecoveredBusiness(Bound original, Function<Bound, ? extends Business> assembly,
            io.tapstate.spi.store.StorageRecoveryCommands.Plan running, Runnable beforePublication) {
        requireOriginal(original);
        if (business != null) return;
        if (cleanupUnknown) throw unavailable();
        verifyCompletedSetup(original);
        verifyRecoveryPublication(original, running);
        original.requireAuthentication(); original.verifyBusiness();
        Business candidate = Objects.requireNonNull(Objects.requireNonNull(assembly, "assembly").apply(original), "business assembly");
        try {
            candidate.start();
            if (running != null) candidate.awaitAvailability();
            requireOriginal(original);
            verifyCompletedSetup(original);
            original.requireActive();
            verifyRecoveryPublication(original, running);
            if (running != null && !candidate.available()) throw unavailable();
            beforePublication.run();
            requireOriginal(original);
            business = candidate;
        } catch (RuntimeException | Error failure) {
            closeCandidate(candidate, failure);
            throw failure;
        }
    }

    private void verifyRecoveryPublication(Bound original, io.tapstate.spi.store.StorageRecoveryCommands.Plan running) {
        if (original.preparation.authScope().authGeneration() < 2) return;
        var pending = configuration.recoveryCommands().current(original.preparation.authScope());
        if (running != null) {
            if (running.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING
                    || !running.originalScope().equals(original.preparation.authScope()) || pending.filter(running::equals).isEmpty()) throw unavailable();
        } else if (pending.filter(plan -> plan.originalScope().equals(original.preparation.authScope())
                && plan.phase() != io.tapstate.spi.store.StorageRecoveryCommands.Phase.COMPLETE
                && plan.manifest().selection().extent()!=io.tapstate.spi.store.StorageRecoveryCommands.Extent.PIPELINE).isPresent()) throw unavailable();
    }

    synchronized boolean businessStarted() { return !closed && business instanceof Business actual && actual.available(); }
    synchronized Optional<Business> business() {
        requireOpen(); return business instanceof Business actual ? Optional.of(actual) : Optional.empty();
    }

    private void verifyCompletedSetup(Bound original) {
        if (loadBound().orElseThrow(StorageRuntime::unavailable) != original) throw unavailable();
        var status = configuration.setupStates().load();
        var setup = original.binding.setup();
        if (status.phase() != StorageSetupStateStore.Phase.COMPLETE
                || status.bindingVersion() != original.binding.bindingVersion()
                || !status.scope().clusterId().equals(setup.clusterId())
                || !status.scope().installationId().equals(setup.installationId())
                || !status.scope().operationId().equals(setup.operationId())) throw unavailable();
    }

    private void requireOriginal(Bound original) {
        requireOpen();
        if (bound != original || original == null) throw unavailable();
        original.requireActive();
    }

    private void requireOpen() { if (closed) throw unavailable(); ownerActive.run(); }

    final class Bound {
        private final StorageBinding binding;
        private final CachePreparation preparation;
        private final MongoCacheDomain cache;
        private volatile boolean active = true;
        private volatile boolean authReady;
        private volatile boolean businessReady;

        private Bound(StorageBinding binding, CachePreparation preparation, MongoCacheDomain cache) {
            this.binding = binding; this.preparation = preparation; this.cache = Objects.requireNonNull(cache, "cache");
            var scope = preparation.authScope();
            if (!binding.setup().clusterId().equals(scope.clusterId()) || binding.bindingVersion() != scope.bindingVersion()
                    || binding.mode() != scope.mode()) throw new IllegalArgumentException("original cache scope disagrees with binding");
        }
        StorageBinding binding() { requireActive(); return binding; }
        CachePreparation preparation() { requireActive(); return preparation; }
        MongoCacheDomain cache() { requireActive(); return cache; }
        void requireActive() { requireOpen(); if (!active) throw unavailable(); }
        void verifyAuthentication() {
            requireActive();
            try { cache.loadAuthScope(preparation.authScope()); requireActive(); authReady = true; }
            catch (RuntimeException | Error unavailable) { authReady = false; throw unavailable; }
        }
        void verifyBusiness() {
            requireActive();
            try { cache.loadBusinessScope(preparation.businessScope()); requireActive(); businessReady = true; }
            catch (RuntimeException | Error unavailable) { businessReady = false; throw unavailable; }
        }
        void requireAuthentication() { requireActive(); if (!authReady) throw unavailable(); }
        void requireBusiness() { requireActive(); if (!businessReady) throw unavailable(); }
        @Override public String toString() { return "BoundStorageRuntime[redacted]"; }
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        if (bound != null) bound.active = false;
        Throwable failure = null;
        if (business != null) {
            try { business.close(); } catch (Exception | Error stopped) { failure = stopped; }
        }
        if (bound != null) {
            try { bound.cache.close(); } catch (RuntimeException | Error stopped) {
                if (failure == null) failure = stopped; else if (failure != stopped) failure.addSuppressed(stopped);
            }
        }
        if (configuration != null) {
            try { configuration.close(); } catch (RuntimeException | Error stopped) {
                if (failure == null) failure = stopped; else if (failure != stopped) failure.addSuppressed(stopped);
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure != null) throw unavailable();
    }

    private void closeCandidate(AutoCloseable candidate, Throwable failure) {
        if (candidate == null) return;
        try { candidate.close(); } catch (Exception | Error cleanup) {
            cleanupUnknown = true;
            if (cleanup != failure) failure.addSuppressed(cleanup);
        }
    }
    static TapstateException unavailable() { return new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null); }
}
