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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Owns CONFIG independently from one original cache connection and its admitted runtime resources. */
final class StorageRuntime implements AutoCloseable {
    interface Business extends AutoCloseable {
        /** Resources are registered with this owner before startup creates members or background work. */
        void start();
        default boolean available() { return true; }
        /** A candidate may wait for its existing membership gate; it must never override that gate. */
        default void awaitAvailability() {}
        /** Called after original first-setup readiness; hands off without waiting for the probe. */
        default void afterInitialReady() {}
        /** Only the main application close event invokes this before invalidating original runtime proofs. */
        default void beforeOrderlyClose() {}
        @Override void close();
    }
    private final MongoConfigurationDomain configuration;
    private final Function<StorageBinding, MongoCacheDomain> connections;
    // Lifecycle writers synchronize; readers capture one identity whose readiness flags are volatile.
    @SuppressWarnings("java:S3077")
    private volatile Bound bound;
    // Liveness reads publish only the owner reference; the owner guards its own lifecycle.
    @SuppressWarnings("java:S3077")
    private volatile AutoCloseable business;
    // Creating owners are retained for cleanup, but are not published as recovered business state.
    private final AtomicReference<Business> creatingBusiness = new AtomicReference<>();
    private volatile boolean closed;
    private volatile boolean stopping;
    private final ShutdownAwait shutdown = new ShutdownAwait();
    private final AdmissionDrain operations = new AdmissionDrain();
    private final AtomicReference<CompletableFuture<Void>> businessOpening =
            new AtomicReference<>(CompletableFuture.completedFuture(null));
    private volatile boolean cleanupUnknown;
    private final Runnable ownerActive;

    StorageRuntime(MongoConfigurationDomain configuration, Function<StorageBinding, MongoCacheDomain> connections) {
        this(configuration, connections, () -> {});
    }
    StorageRuntime(MongoConfigurationDomain configuration, Function<StorageBinding, MongoCacheDomain> connections, Runnable ownerActive) {
        this.configuration = configuration;
        this.connections = Objects.requireNonNull(connections, "connections");
        this.ownerActive = Objects.requireNonNull(ownerActive, "ownerActive");
    }

    Optional<MongoConfigurationDomain> configuration() {
        requireOpen(); return Optional.ofNullable(configuration);
    }

    MongoConfigurationDomain requireConfiguration() {
        return configuration().orElseThrow(StorageRuntime::unavailable);
    }

    /** An ordinary restart only loads retained identities; it never creates auth or business facts. */
    Optional<Bound> loadBound() { return lifecycle(this::loadBoundOwned); }
    private Optional<Bound> loadBoundOwned() {
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
            requireOpen();
            return Optional.of(candidate);
        } catch (RuntimeException | Error failure) {
            try { cache.close(); } catch (RuntimeException | Error cleanup) {
                cleanupUnknown = true;
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    Optional<Bound> current() { requireOpen();var current=bound;requireOpen();return Optional.ofNullable(current); }
    /** Read-only progress compares a published identity without joining candidate startup. */
    Optional<Bound> boundForObservation() {
        requireOpen(); var original = bound; requireOpen(); return Optional.ofNullable(original);
    }
    boolean cleanupRequired() { return cleanupUnknown; }

    /** Retires this process's original owners before loading a newly published authentication scope. */
    Bound restoreAuthentication() { return lifecycle(this::restoreAuthenticationOwned); }
    private Bound restoreAuthenticationOwned() {
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
            closeCandidate(candidate, failure);
        }
        return loadBound().orElseThrow(StorageRuntime::unavailable);
    }

    private void closeCandidate(MongoCacheDomain candidate, Throwable primary) {
        try { candidate.close(); } catch (RuntimeException | Error cleanup) {
            cleanupUnknown = true;
            if (primary != null) { if (primary != cleanup) primary.addSuppressed(cleanup); }
            else throw cleanup;
        }
    }

    private void retireBound() {
        Bound original = bound;
        AutoCloseable owned = business;
        if (original != null) original.active = false;
        Throwable failure = null;
        if (owned != null) {
            try { owned.close(); } catch (Exception | Error cleanup) { failure = cleanup; }
        }
        if (failure == null && original != null) {
            try { original.cache.close(); } catch (RuntimeException | Error cleanup) {
                failure = cleanup;
            }
        }
        if (failure != null) {
            cleanupUnknown = true;
            if (failure instanceof Error error) throw error;
            var unavailable = unavailable(); unavailable.addSuppressed(failure); throw unavailable;
        }
        bound = null;business = null;
    }

    /** The caller passes the original owner, so a late callback cannot activate or close its successor. */
    void startBusiness(Bound original, Function<Bound, ? extends Business> assembly) {
        startRecoveredBusiness(original, assembly, null, () -> {});
    }

    void startRecoveredBusiness(Bound original, Function<Bound, ? extends Business> assembly,
            io.tapstate.spi.store.StorageRecoveryCommands.Plan running, Runnable beforePublication) {
        lifecycle(()->{startBusinessOwned(original, assembly, running, beforePublication, running != null);return null;});
    }

    void startInitialBusiness(Bound original, Function<Bound, ? extends Business> assembly) {
        lifecycle(()->{startBusinessOwned(original, assembly, null, () -> {}, true);return null;});
    }

    private void startBusinessOwned(Bound original, Function<Bound, ? extends Business> assembly,
            io.tapstate.spi.store.StorageRecoveryCommands.Plan running, Runnable beforePublication, boolean awaitReady) {
        var opening = new java.util.concurrent.CompletableFuture<Void>();
        businessOpening.set(opening);
        try { startBusinessAttempt(original, assembly, running, beforePublication, awaitReady); }
        finally { opening.complete(null); }
    }

    private void startBusinessAttempt(Bound original, Function<Bound, ? extends Business> assembly,
            io.tapstate.spi.store.StorageRecoveryCommands.Plan running, Runnable beforePublication, boolean awaitReady) {
        requireOriginal(original);
        if (cleanupUnknown) throw unavailable();
        if (business != null) {
            if (awaitReady && !businessStarted()) throw unavailable();
            return;
        }
        if (cleanupUnknown) throw unavailable();
        verifyCompletedSetup(original);
        verifyRecoveryPublication(original, running);
        original.requireAuthentication(); original.verifyBusiness();
        Business candidate = Objects.requireNonNull(Objects.requireNonNull(assembly, "assembly").apply(original), "business assembly");
        creatingBusiness.set(candidate);
        try {
            candidate.start();
            if (awaitReady) candidate.awaitAvailability();
            requireOriginal(original);
            verifyCompletedSetup(original);
            original.requireActive();
            verifyRecoveryPublication(original, running);
            if (awaitReady && !candidate.available()) throw unavailable();
            beforePublication.run();
            requireOriginal(original);
            business = candidate;
            creatingBusiness.set(null);
        } catch (RuntimeException | Error failure) {
            closeCandidate(candidate, failure);
            if (!cleanupUnknown) creatingBusiness.compareAndSet(candidate, null);
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
                && plan.manifest().selection().extent()==io.tapstate.spi.store.StorageRecoveryCommands.Extent.CLUSTER).isPresent()) throw unavailable();
    }

    boolean businessStarted() { return !closed && !stopping && !cleanupUnknown && business instanceof Business actual && actual.available(); }
    Optional<Business> business() {
        requireOpen(); return business instanceof Business actual ? Optional.of(actual) : Optional.empty();
    }

    /** Liveness observes only the published owner and never waits for candidate startup or does IO. */
    Optional<Business> businessForLiveness() {
        AutoCloseable current = business;
        return !closed && current instanceof Business actual ? Optional.of(actual) : Optional.empty();
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

    private void requireOpen() { if (closed || stopping) throw unavailable(); ownerActive.run(); }
    private <T>T lifecycle(java.util.function.Supplier<T> action){
        if(!operations.enter())throw unavailable();
        try {synchronized(this){return action.get();}}finally{operations.leave();}
    }

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
            try { cache.verifyBusinessReady(preparation.businessScope()); requireActive(); businessReady = true; }
            catch (RuntimeException | Error unavailable) { businessReady = false; throw unavailable; }
        }
        void requireAuthentication() { requireActive(); if (!authReady) throw unavailable(); }
        void requireBusiness() { requireActive(); if (!businessReady) throw unavailable(); }
        @Override public String toString() { return "BoundStorageRuntime[redacted]"; }
    }

    @Override public void close() { close(ShutdownAwait.deadline()); }

    void close(long deadline) {
        stopping = true;
        operations.stop();
        if (bound != null) bound.active = false;
        shutdown.complete(deadline, () -> {
            try {
                operations.await(deadline);
                ShutdownAwait.future(businessOpening.get(), deadline);
                var creating = creatingBusiness.get();
                if (creating != null) {
                    creating.close();
                    creatingBusiness.compareAndSet(creating, null);
                }
                if (business != null) business.close();
                if (bound != null) bound.cache.close();
                if (configuration != null) configuration.close();
                closed = true;cleanupUnknown = false;
            } catch (Error error) { cleanupUnknown = true;throw error; }
            catch (Exception failure) {
                cleanupUnknown = true;
                var unavailable = unavailable();unavailable.addSuppressed(failure);throw unavailable;
            }
        });
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
