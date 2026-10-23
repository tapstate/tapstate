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
    private volatile Optional<StorageBinding.Connection> endpoint;
    private final java.util.function.Supplier<MongoConfigurationDomain> configurationLoader;
    private final Function<MongoConfigurationDomain, StorageRuntime> runtimes;
    private final Function<MongoConfigurationDomain, StorageAuthentication> authentications;
    private final CloudConfigurationInitialization initialization;
    private final byte[] authenticationKey;
    private volatile Phase phase;
    private volatile boolean closed;
    private FirstCacheCreation attemptedCreation;
    private Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> retainedAssembly;
    private java.util.function.Supplier<StorageSetupService.DefaultTarget> retainedTarget;
    private StorageBindingCommit setupInvocation;
    private java.util.concurrent.ExecutorService recoveryWorker;
    private volatile boolean recoveryQueued;
    private volatile boolean recoveryFailed;
    private volatile StorageRecoveryCommands.Plan failedRecovery;
    private volatile TapstateException recoveryFailure;
    private final Runnable installationActive;

    StorageRoot(MongoProperties mongo, CloudRuntimeSettings cloud, CloudSdkBridge bridge, BootId boot,
            MetricsHistoryProperties history, TokenSecrets secrets, Clock clock, byte[] signingKey) {
        this(mongo, cloud, bridge, boot, history, secrets, clock, signingKey, () -> {});
    }
    StorageRoot(MongoProperties mongo, CloudRuntimeSettings cloud, CloudSdkBridge bridge, BootId boot,
            MetricsHistoryProperties history, TokenSecrets secrets, Clock clock, byte[] signingKey, Runnable installationActive) {
        this.installationActive = Objects.requireNonNull(installationActive);
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
        }, this::requireOpen);
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
    Optional<org.springframework.boot.availability.ApplicationAvailability> businessAvailability() {
        StorageRuntime current = runtime;
        if (closed || current == null) return Optional.empty();
        var business = current.businessForLiveness().orElse(null);
        return business instanceof NativeBusinessContext nativeBusiness ? nativeBusiness.availability() : Optional.empty();
    }
    StorageAuthentication authentication() { requireOpen(); return authentication; }
    Optional<StorageBinding.Connection> configurationEndpoint() {
        requireOpen(); return runtime.configuration().isPresent() ? endpoint : Optional.empty();
    }
    void requireActive() { requireOpen(); }
    StorageBinding.Mode mode() { return cloud.cloud() ? StorageBinding.Mode.CLOUD : StorageBinding.Mode.OP; }

    synchronized StorageSetupService.Status coordinateSetup(java.util.function.Supplier<StorageSetupService.Status> action) {
        requireOpen(); return action.get();
    }
    synchronized <T> T coordinateInstallation(java.util.function.Supplier<T> action) { requireOpen(); return action.get(); }
    synchronized boolean originalSetupCreationAvailable() {
        return setupInvocation != null && setupInvocation.firstCreation().isPresent()
                && attemptedCreation != setupInvocation.firstCreation().orElseThrow();
    }

    /** Transfers the original installer-owned CONFIG client; the installer retains no second runtime. */
    synchronized void installOnPremConfiguration(MongoConfigurationDomain candidate, StorageBinding.Connection connection) {
        requireOpen();
        if (cloud.cloud() || runtime.configuration().isPresent() || runtime.cleanupRequired()) throw StorageRuntime.unavailable();
        var nextRuntime = runtimes.apply(candidate);
        var nextAuthentication = authentications.apply(candidate);
        authentication.close(); runtime.close();
        endpoint = Optional.of(connection); runtime = nextRuntime; authentication = nextAuthentication; phase = Phase.SETUP_REQUIRED;
    }

    /** Continues only the original committed setup. No request owns or closes the retained CACHE client. */
    synchronized void continueCloudSetup(StorageBindingCommit committed, StorageSetupService.DefaultTarget target,
            Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> assembly) {
        requireOpen();
        if (!cloud.cloud() || committed.binding().mode() != StorageBinding.Mode.CLOUD || runtime.cleanupRequired())
            throw StorageRuntime.unavailable();
        var original = prepareInitialSetup(committed, target);
        authentication.install(original); phase = Phase.AUTH_READY;
        runtime.startBusiness(original, assembly); phase = Phase.BUSINESS_READY;
    }

    /** OP prepares its original storage but keeps authentication and business behind a controlled restart. */
    synchronized void continueOnPremSetup(StorageBindingCommit committed, StorageSetupService.DefaultTarget target) {
        requireOpen();
        if (cloud.cloud() || committed.binding().mode() != StorageBinding.Mode.OP || runtime.cleanupRequired()) throw StorageRuntime.unavailable();
        prepareInitialSetup(committed, target);
        phase = Phase.SETUP_REQUIRED;
    }

    private StorageRuntime.Bound prepareInitialSetup(StorageBindingCommit committed, StorageSetupService.DefaultTarget target) {
        setupInvocation = committed;
        var original = runtime.loadBound().orElseThrow(StorageRuntime::unavailable);
        Runnable active = () -> {
            requireOpen(); original.requireActive();
            if (runtime.current().orElseThrow(StorageRuntime::unavailable) != original
                    || !original.binding().equals(committed.binding()) || !original.preparation().equals(committed.preparation())
                    || !runtime.requireConfiguration().bindings().load().orElseThrow(StorageRuntime::unavailable).equals(committed.binding())
                    || !runtime.requireConfiguration().bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable).equals(committed.preparation()))
                throw StorageRuntime.unavailable();
            runtime.requireConfiguration().requireOriginalInitialPreparation(committed.preparation());
        };
        active.run();
        var configuration = runtime.requireConfiguration();
        if (configuration.setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE) {
            if (committed.preparation().authScope().authGeneration() > 1) throw StorageRuntime.unavailable();
            phase = Phase.SETUP_REQUIRED;
            // Reload the saved encrypted definition and its trust material; a probe's live client is never adopted.
            var source = configuration.artifacts().get(committed.binding().defaultSourceId())
                    .filter(SourceResource.class::isInstance).map(SourceResource.class::cast).orElseThrow(StorageRuntime::unavailable);
            if (!(source.config().get("uri") instanceof String uri)
                    || !Set.of("mongodb", "mongodb-atlas").contains(source.connector())
                    || cloud.cloud() && !source.connector().equals("mongodb-atlas"))
                throw StorageRuntime.unavailable();
            var master = new StorageBinding.Connection(source.connector().equals("mongodb-atlas")
                    ? StorageBinding.Connection.Kind.ATLAS : StorageBinding.Connection.Kind.MONGODB, uri, MongoDatabaseNames.fromUri(uri),
                    source.config().get(StorageSetupService.CA_BUNDLE_FIELD) instanceof String ca ? ca : null);
            target.test(master, source, target.identity(master)); active.run();
            if (committed.firstCreation().isPresent() && attemptedCreation != committed.firstCreation().orElseThrow()) {
                attemptedCreation = committed.firstCreation().orElseThrow();
                original.cache().prepareFirstAuth(attemptedCreation);
            } else original.cache().resumeInitialAuth(committed.preparation(), active);
            original.verifyAuthentication();
            original.cache().prepareInitialBusiness(committed.preparation(), active);
            original.verifyBusiness(); active.run();
            var currentSetup = configuration.setupStates().load().scope();
            if (!currentSetup.clusterId().equals(committed.binding().setup().clusterId())
                    || !currentSetup.installationId().equals(committed.binding().setup().installationId())
                    || !currentSetup.operationId().equals(committed.binding().setup().operationId())) throw StorageRuntime.unavailable();
            configuration.bindings().guardedBy(active).complete(currentSetup);
        }
        active.run();
        return original;
    }

    /** Explicit startup/operation continuation only. Status reads never connect or initialize a cache. */
    synchronized void loadExisting(Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> assembly) {
        loadExisting(assembly, null);
    }
    synchronized void loadExisting(Function<StorageRuntime.Bound, ? extends StorageRuntime.Business> assembly,
            java.util.function.Supplier<StorageSetupService.DefaultTarget> target) {
        requireOpen();
        retainedAssembly = Objects.requireNonNull(assembly);
        retainedTarget = target;
        if (runtime.configuration().isEmpty()) return;
        try {
            var original = runtime.loadBound();
            if (original.isEmpty()) { phase = Phase.SETUP_REQUIRED; return; }
            if (cloud.cloud() && runtime.requireConfiguration().setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE) {
                continueIncompleteCloud(); return;
            }
            phase = Phase.CACHE_UNAVAILABLE;
            try { authentication.install(original.orElseThrow()); }
            catch (TapstateException unavailable) { restoreAuthentication(); }
            original = runtime.current(); phase = Phase.AUTH_READY;
            wakeRecovery();
            if (runtime.requireConfiguration().setupStates().load().phase() == StorageSetupStateStore.Phase.COMPLETE) {
                runtime.startBusiness(original.orElseThrow(), assembly); phase = Phase.BUSINESS_READY;
            }
        } catch (TapstateException unavailable) {
            if (unavailable.code() == BootError.NODE_ID_IN_USE) throw unavailable;
            if (phase != Phase.AUTH_READY) phase = Phase.CACHE_UNAVAILABLE;
        }
    }

    synchronized LocalAuthenticationContext localAuthentication() {
        requireOpen();
        if (runtime.configuration().isEmpty()) throw StorageRuntime.unavailable();
        LocalAuthenticationContext context;
        try { context = authentication.local(); }
        catch (TapstateException unavailable) {
            if (unavailable.code() != ControlError.STORAGE_UNAVAILABLE) throw unavailable;
            restoreAuthentication(); context = authentication.local();
        }
        resumeRetainedBusiness();wakeRecovery();return context;
    }

    synchronized ManagedAuthenticationContext managedAuthentication() {
        requireOpen();
        if (runtime.configuration().isEmpty()) loadConfiguration();
        if (runtime.configuration().isEmpty()) throw StorageRuntime.unavailable();
        if (cloud.cloud() && runtime.requireConfiguration().setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE)
            continueIncompleteCloud();
        ManagedAuthenticationContext context;
        try { context = authentication.managed(); }
        catch (TapstateException unavailable) {
            if (unavailable.code() != ControlError.STORAGE_UNAVAILABLE) throw unavailable;
            restoreAuthentication(); context = authentication.managed();
        }
        resumeRetainedBusiness(); wakeRecovery(); return context;
    }

    private void continueIncompleteCloud() {
        phase = Phase.SETUP_REQUIRED;
        if (retainedAssembly == null) return;
        var bound = runtime.loadBound();
        if (bound.isEmpty()) return;
        var original = bound.orElseThrow();
        if (original.preparation().authScope().authGeneration() == 1 && retainedTarget != null) {
            var committed = setupInvocation != null && setupInvocation.binding().equals(original.binding())
                    && setupInvocation.preparation().equals(original.preparation()) ? setupInvocation
                    : new StorageBindingCommit(original.binding(), original.preparation(), Optional.empty());
            try { continueCloudSetup(committed, retainedTarget.get(), retainedAssembly); return; }
            catch (TapstateException unavailable) { /* The original invocation is retained; never manufacture another. */ }
            if (committed.firstCreation().isPresent() && attemptedCreation != committed.firstCreation().orElseThrow()) return;
        }
        try {
            var restored = runtime.restoreAuthentication();
            if (restored.preparation().authScope().authGeneration() > 1) {
                if (restored != original) authentication.invalidateFormal();
                authentication.install(restored); phase = Phase.AUTH_READY; wakeRecovery();
            }
        } catch (TapstateException unavailable) { phase = Phase.CACHE_UNAVAILABLE; }
    }

    synchronized StorageRecoveryService recoveryService(Clock clock) {
        var configuration = runtime.requireConfiguration();
        return new StorageRecoveryService(configuration.recoveryCommands(), new StorageRecoveryService.Inspector() {
            @Override public StorageRecoveryCommands.Manifest inspect(CacheAuthScope scope, StorageRecoveryCommands.Selection selection) {
                synchronized (StorageRoot.this) { return recoveryManifest(scope, selection, true); }
            }
            @Override public void requireCurrent(CacheAuthScope scope, StorageRecoveryCommands.Manifest manifest) {
                synchronized (StorageRoot.this) {
                    var actual = manifest.selection().extent()==StorageRecoveryCommands.Extent.PIPELINE
                            ? pipelineRecovery(scope).inspectCurrent(manifest.selection()).manifest():recoveryManifest(scope, manifest.selection(), true);
                    if (!actual.definitionHashes().equals(manifest.definitionHashes()) || !actual.dependencyHash().equals(manifest.dependencyHash()))
                        throw StorageRuntime.unavailable();
                }
            }
            @Override public StorageRecoveryService.Presentation present(StorageRecoveryCommands.Plan original) {
                synchronized(StorageRoot.this) {
                    String failure=failedRecovery!=null && recoveryFailure!=null && failedRecovery.operationId().equals(original.operationId())
                            && failedRecovery.originalScope().equals(original.originalScope()) && failedRecovery.controlRevision()==original.controlRevision()
                            && original.phase()!=StorageRecoveryCommands.Phase.COMPLETE?recoveryFailure.code().code():null;
                    if(original.manifest().selection().extent()!=StorageRecoveryCommands.Extent.PIPELINE)
                        return new StorageRecoveryService.Presentation(runtime.current().map(bound -> bound.preparation().authScope().equals(original.originalScope())).orElse(false),List.of(),failure);
                    try {
                        var nativeRecovery=pipelineRecovery(original.originalScope());
                        if(failure==null)failure=nativeRecovery.failureCode(original);
                        var inspected=nativeRecovery.inspectCurrent(original.manifest().selection());
                        boolean current=inspected.manifest().definitionHashes().equals(original.manifest().definitionHashes())
                                && inspected.manifest().dependencyHash().equals(original.manifest().dependencyHash());
                        return new StorageRecoveryService.Presentation(current,current?inspected.targets().stream().map(target ->
                                new StorageRecoveryService.TargetPolicy(target.node(),target.source(),target.policy().name())).toList():List.of(),failure);
                    } catch(TapstateException unavailable) {return new StorageRecoveryService.Presentation(false,List.of(),failure);}
                }
            }
            @Override public void verifyExternal(CacheAuthScope scope,StorageRecoveryCommands.Manifest manifest) {
                synchronized(StorageRoot.this) {
                    if(manifest.selection().extent()!=StorageRecoveryCommands.Extent.PIPELINE)return;
                    var actual=pipelineRecovery(scope).verifyExternal(manifest.selection()).manifest();
                    if(!actual.definitionHashes().equals(manifest.definitionHashes()) || !actual.dependencyHash().equals(manifest.dependencyHash()))throw StorageRuntime.unavailable();
                }
            }
        }, new AuditGate(configuration.audit(), clock), this::scheduleRecovery);
    }

    private StorageRuntime.Bound requireRecoveryOwner(CacheAuthScope scope) {
        requireOpen();
        if (runtime.cleanupRequired() || retainedTarget == null || retainedAssembly == null) throw StorageRuntime.unavailable();
        var original = runtime.current().orElseThrow(StorageRuntime::unavailable);
        original.requireActive();
        if (!original.preparation().authScope().equals(scope)) throw StorageRuntime.unavailable();
        runtime.requireConfiguration().requireAuthenticationPublished(original.preparation());
        original.verifyAuthentication(); return original;
    }

    private NativePipelineRecovery pipelineRecovery(CacheAuthScope scope) {
        requireRecoveryOwner(scope);
        if(!(runtime.business().orElseThrow(StorageRuntime::unavailable) instanceof NativeBusinessContext nativeBusiness))throw StorageRuntime.unavailable();
        return nativeBusiness.pipelineRecovery();
    }

    private StorageRecoveryCommands.Manifest recoveryManifest(CacheAuthScope scope, StorageRecoveryCommands.Selection selection, boolean authOnly) {
        if(selection.extent()==StorageRecoveryCommands.Extent.PIPELINE)return pipelineRecovery(scope).inspect(selection).manifest();
        if (runtime.requireConfiguration().setupStates().load().phase()!=StorageSetupStateStore.Phase.COMPLETE)
            return initialManifest(scope,selection,authOnly);
        var original=requireRecoveryOwner(scope);
        if (selection.extent()!=StorageRecoveryCommands.Extent.CLUSTER || runtime.business().isPresent()) throw StorageRuntime.unavailable();
        if (authOnly) original.cache().verifyAuthenticationOnly(scope);
        var configuration=runtime.requireConfiguration();
        var changes=configuration.cacheRecoveries().inspectCluster(scope);
        var hashes=new TreeMap<String,String>();
        for(var resource:configuration.artifacts().list()) hashes.put(resource.id(),io.tapstate.core.model.canonical.CanonicalHash.of(resource));
        var affected=changes.stream().map(change -> change.targetScope().pipelineId()).collect(java.util.stream.Collectors.toSet());
        return new StorageRecoveryCommands.Manifest(selection,affected,hashes,CacheRecoveryStore.clusterDependencyHash(scope,changes),configuration.configurationRevision());
    }

    private StorageRecoveryCommands.Manifest initialManifest(CacheAuthScope scope, StorageRecoveryCommands.Selection selection, boolean authOnly) {
        var original = requireRecoveryOwner(scope);
        if (selection.extent() != StorageRecoveryCommands.Extent.CLUSTER || runtime.business().isPresent()) throw StorageRuntime.unavailable();
        var configuration = runtime.requireConfiguration();
        var source = configuration.initialCloudRecoverySource(scope);
        if (authOnly) original.cache().verifyAuthenticationOnly(scope);
        var master = master(source);
        var connector = retainedTarget.get().identity(master);
        String dependency = io.tapstate.core.model.canonical.CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(List.of(
                "initial-cloud-binding", original.binding().setup().installationId(), original.binding().setup().operationId(),
                scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), scope.authGeneration(), connector.artifactHash(), connector.specHash())));
        return new StorageRecoveryCommands.Manifest(selection, Set.of(), Map.of(source.id(), io.tapstate.core.model.canonical.CanonicalHash.of(source)),
                dependency, configuration.configurationRevision());
    }

    private void wakeRecovery() {
        if (runtime.cleanupRequired() || runtime.current().isEmpty()) return;
        var scope = runtime.current().orElseThrow().preparation().authScope();
        if (scope.authGeneration() < 2) return;
        runtime.requireConfiguration().recoveryCommands().current(scope).filter(plan -> plan.originalScope().equals(scope)).ifPresent(plan -> scheduleRecovery(plan,false));
    }

    private synchronized void scheduleRecovery(StorageRecoveryCommands.Plan original) {scheduleRecovery(original,true);}
    private synchronized void scheduleRecovery(StorageRecoveryCommands.Plan original,boolean explicitContinuation) {
        requireOpen();
        boolean pipeline=original.manifest().selection().extent()==StorageRecoveryCommands.Extent.PIPELINE;
        if(pipeline && original.phase()==StorageRecoveryCommands.Phase.RUNNING && !explicitContinuation)return;
        if (original.phase()==StorageRecoveryCommands.Phase.COMPLETE) {
            requireRecoveryOwner(original.originalScope());
            if(runtime.requireConfiguration().recoveryCommands().find(original.originalScope(),original.operationId()).filter(original::equals).isEmpty())
                throw StorageRuntime.unavailable();
            resumeRetainedBusiness();return;
        }
        if (original.phase() != StorageRecoveryCommands.Phase.CONFIRMED && original.phase() != StorageRecoveryCommands.Phase.RUNNING) return;
        requireRecoveryOwner(original.originalScope());
        if (recoveryQueued) return;
        long acceptedRevision=pipeline?pipelineRecovery(original.originalScope()).acceptedIntentRevision(original):-1;
        if (recoveryWorker == null) recoveryWorker = java.util.concurrent.Executors.newSingleThreadExecutor(
                Thread.ofPlatform().daemon(true).name("cache-recovery").factory());
        recoveryQueued = true; recoveryFailed = false; recoveryFailure = null;failedRecovery=null;
        recoveryWorker.execute(() -> {
            synchronized (StorageRoot.this) {
                try {
                    if(pipeline)pipelineRecovery(original.originalScope()).start(original,explicitContinuation,acceptedRevision);
                    else if(runtime.requireConfiguration().setupStates().load().phase()==StorageSetupStateStore.Phase.COMPLETE && !isInitialRecovery(original)) runStoppedRecovery(original);
                    else runInitialRecovery(original);
                }
                catch (TapstateException unavailable) { recoveryFailure = unavailable;failedRecovery=original;recoveryFailed=!pipeline; }
                finally { recoveryQueued = false; }
            }
        });
    }

    private boolean isInitialRecovery(StorageRecoveryCommands.Plan original) {
        if (!cloud.cloud() || original.manifest().selection().extent()!=StorageRecoveryCommands.Extent.CLUSTER
                || !original.manifest().affectedPipelineIds().isEmpty())return false;
        try {
            var expected=initialManifest(original.originalScope(),original.manifest().selection(),false);
            return expected.definitionHashes().equals(original.manifest().definitionHashes()) && expected.dependencyHash().equals(original.manifest().dependencyHash());
        } catch(TapstateException unavailable) {return false;}
    }

    /** Confirmation includes operator isolation of all old members and external in-flight writes. */
    private void runStoppedRecovery(StorageRecoveryCommands.Plan requested) {
        var original=requireRecoveryOwner(requested.originalScope());var configuration=runtime.requireConfiguration();
        var commands=configuration.recoveryCommands();
        var current=commands.find(requested.originalScope(),requested.operationId()).orElseThrow(StorageRuntime::unavailable);
        if(current.phase()==StorageRecoveryCommands.Phase.COMPLETE){resumeRetainedBusiness();return;}
        if(current.manifest().selection().extent()!=StorageRecoveryCommands.Extent.CLUSTER)throw StorageRuntime.unavailable();
        var inspected=recoveryManifest(current.originalScope(),current.manifest().selection(),current.phase()==StorageRecoveryCommands.Phase.CONFIRMED);
        if(!inspected.definitionHashes().equals(current.manifest().definitionHashes()) || !inspected.dependencyHash().equals(current.manifest().dependencyHash()))
            throw StorageRuntime.unavailable();
        Runnable local=() -> {
            requireOpen();original.requireActive();
            if(runtime.current().orElseThrow(StorageRuntime::unavailable)!=original || runtime.cleanupRequired() || runtime.business().isPresent())
                throw StorageRuntime.unavailable();
        };
        local.run();configuration.cacheRecoveries().guardedBy(local).stageCluster(current);
        var running=commands.find(current.originalScope(),current.operationId()).orElseThrow(StorageRuntime::unavailable);
        configuration.prepareStoppedRecovery(original.cache(),running,local);
        original.verifyBusiness();local.run();
        runtime.startRecoveredBusiness(original,retainedAssembly,running,() -> commands.guardedBy(local).complete(running));
        phase=Phase.BUSINESS_READY;
    }

    private void runInitialRecovery(StorageRecoveryCommands.Plan requested) {
        var original = requireRecoveryOwner(requested.originalScope());
        var configuration = runtime.requireConfiguration();
        var commands = configuration.recoveryCommands();
        var current = commands.find(requested.originalScope(), requested.operationId()).orElseThrow(StorageRuntime::unavailable);
        if (current.phase() == StorageRecoveryCommands.Phase.COMPLETE) return;
        var inspected = initialManifest(current.originalScope(), current.manifest().selection(), current.phase() == StorageRecoveryCommands.Phase.CONFIRMED);
        if (!inspected.definitionHashes().equals(current.manifest().definitionHashes())
                || !inspected.dependencyHash().equals(current.manifest().dependencyHash())) throw StorageRuntime.unavailable();
        var source = configuration.initialCloudRecoverySource(current.originalScope());
        var target = retainedTarget.get(); var master = master(source);
        target.test(master, source, target.identity(master));
        var running = current.phase() == StorageRecoveryCommands.Phase.CONFIRMED ? commands.start(current) : current;
        Runnable active = () -> {
            requireOpen(); original.requireActive();
            if (runtime.current().orElseThrow(StorageRuntime::unavailable) != original || runtime.cleanupRequired()
                    || !commands.find(running.originalScope(), running.operationId()).filter(running::equals).isPresent()) throw StorageRuntime.unavailable();
            configuration.requireAuthenticationPublished(original.preparation());
        };
        configuration.prepareInitialCloudRecovery(original.cache(), running, active);
        original.verifyBusiness(); active.run();
        target.test(master, source, target.identity(master)); active.run();
        configuration.bindings().guardedBy(active).complete(original.binding().setup());
        runtime.startRecoveredBusiness(original, retainedAssembly, running, () -> commands.complete(running));
        phase = Phase.BUSINESS_READY;
    }

    private static StorageBinding.Connection master(SourceResource source) {
        if (!(source.config().get("uri") instanceof String uri)) throw StorageRuntime.unavailable();
        return new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS, uri, MongoDatabaseNames.fromUri(uri),
                source.config().get(StorageSetupService.CA_BUNDLE_FIELD) instanceof String ca ? ca : null);
    }

    private void resumeRetainedBusiness() {
        if (retainedAssembly == null || runtime.cleanupRequired() || runtime.business().isPresent()) return;
        if (runtime.requireConfiguration().setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE) return;
        try {
            var original = runtime.current().orElseThrow(StorageRuntime::unavailable);
            runtime.startBusiness(original, retainedAssembly);
            phase = Phase.BUSINESS_READY;recoveryFailed=false;recoveryFailure=null;
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
        if (awaitingRebuild) business = recoveryQueued ? StorageControlDispatcher.BusinessPhase.RECOVERING
                : StorageControlDispatcher.BusinessPhase.AWAITING_REBUILD_CONFIRMATION;
        if (recoveryFailed && failedRecovery!=null && preparation.filter(value -> value.authScope().equals(failedRecovery.originalScope())).isPresent())
            business = StorageControlDispatcher.BusinessPhase.UNAVAILABLE;
        if (runtime.cleanupRequired()) business = StorageControlDispatcher.BusinessPhase.CLEANUP_REQUIRED;
        boolean formalRecovery = preparation.map(value -> value.authScope().authGeneration() > 1).orElse(false);
        var pending = formalRecovery ? configuration.recoveryCommands().guardedBy(active).current(preparation.orElseThrow().authScope())
                : Optional.<StorageRecoveryCommands.Plan>empty();
        var actions = new ArrayList<>(List.of("storage.status", "storage.setup.status"));
        if (caller.cacheScope().isPresent()) {
            actions.addAll(List.of("configuration.export", "configuration.import.status"));
            if (admitted) actions.addAll(List.of("configuration.import.preflight", "configuration.import.commit"));
        }
        if (!runtime.cleanupRequired()) {
            if (formalRecovery) {
                actions.add("storage.recovery.status");
                if (status.phase() == StorageSetupStateStore.Phase.COMPLETE || mode() == StorageBinding.Mode.CLOUD) {
                    actions.add("storage.recovery.plan");
                    if (pending.filter(plan -> plan.originalScope().equals(preparation.orElseThrow().authScope())).isPresent())
                        actions.add("storage.recovery.confirm");
                }
            } else if (mode() == StorageBinding.Mode.CLOUD && status.phase() != StorageSetupStateStore.Phase.COMPLETE) {
                actions.add("storage.test"); actions.add("storage.setup.commit");
            }
        }
        active.run();
        return new StorageControlDispatcher.Snapshot(mode(), status.scope().clusterId(), pending.map(StorageRecoveryCommands.Plan::operationId)
                .orElseGet(() -> awaitingRebuild ? preparation.orElseThrow().operationId() : status.scope().operationId()),
                status.scope().generation(), status.bindingVersion(), preparation.map(value -> value.authScope().authGeneration()).orElse(0L),
                configuration.configurationRevision(), status.phase().name(), business, health,
                actions, false);
    }

    private void requireOpen() { if (closed) throw StorageRuntime.unavailable(); installationActive.run(); }
    private static String readCa(String path) {
        if (path == null || path.isBlank()) return null;
        try { return java.nio.file.Files.readString(java.nio.file.Path.of(path)); }
        catch (java.io.IOException invalid) { throw StorageRuntime.unavailable(); }
    }
    @Override public synchronized void close() {
        if (closed) return; closed = true;
        if (recoveryWorker != null) recoveryWorker.shutdownNow();
        if (initialization != null) initialization.close();
        try { authentication.close(); runtime.close(); }
        finally { java.util.Arrays.fill(authenticationKey, (byte)0); }
    }
}
