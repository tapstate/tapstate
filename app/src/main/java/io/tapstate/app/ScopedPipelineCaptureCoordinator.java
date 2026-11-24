package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.FailureFact;
import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.srs.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.Phase;

/** Native source owners retain the original Pipeline, capture and physical handoff identities. */
final class ScopedPipelineCaptureCoordinator implements PipelineCaptureCoordinator,AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ScopedPipelineCaptureCoordinator.class);
    interface BoundedLoads {
        boolean complete(ExecutionFence original);
        void forget(ExecutionFence original);
        BoundedLoads NONE=new BoundedLoads(){public boolean complete(ExecutionFence original){return false;}public void forget(ExecutionFence original){}};
    }
    @FunctionalInterface interface CompletedFullLookup {
        Optional<CompletedPipelineFull> find(PipelineIntentScope scope, PipelinePreparationInputs inputs, Map<String, String> dependencies);
        CompletedFullLookup NONE = (scope, inputs, dependencies) -> Optional.empty();
    }
    record Stores(CacheBusinessScope scope,PipelineStoreViews pipelines,SrsStoreViews sources,
            CaptureStatePreparations configuration,CaptureCachePreparations preparations,RequiredNoteStore notes,Predicate<PipelineIntentScope> recoveryPinned,BoundedLoads boundedLoads,
            CompletedFullLookup completedFull) {
        Stores(CacheBusinessScope scope,PipelineStoreViews pipelines,SrsStoreViews sources,CaptureStatePreparations configuration,CaptureCachePreparations preparations,RequiredNoteStore notes) {
            this(scope,pipelines,sources,configuration,preparations,notes,ignored -> false,BoundedLoads.NONE,CompletedFullLookup.NONE);
        }
        Stores(CacheBusinessScope scope,PipelineStoreViews pipelines,SrsStoreViews sources,CaptureStatePreparations configuration,CaptureCachePreparations preparations,RequiredNoteStore notes,Predicate<PipelineIntentScope> recoveryPinned) {
            this(scope,pipelines,sources,configuration,preparations,notes,recoveryPinned,BoundedLoads.NONE,CompletedFullLookup.NONE);
        }
        Stores(CacheBusinessScope scope,PipelineStoreViews pipelines,SrsStoreViews sources,CaptureStatePreparations configuration,CaptureCachePreparations preparations,RequiredNoteStore notes,Predicate<PipelineIntentScope> recoveryPinned,BoundedLoads boundedLoads) {
            this(scope,pipelines,sources,configuration,preparations,notes,recoveryPinned,boundedLoads,CompletedFullLookup.NONE);
        }
        Stores { Objects.requireNonNull(scope); Objects.requireNonNull(pipelines); Objects.requireNonNull(sources); Objects.requireNonNull(configuration); Objects.requireNonNull(preparations); Objects.requireNonNull(notes);Objects.requireNonNull(recoveryPinned);Objects.requireNonNull(boundedLoads);Objects.requireNonNull(completedFull); }
    }
    record Execution(Supplier<Optional<WorkloadClaimFence>> node,ExecutionAuthorization authorization,
            CaptureOwnership ownership,Duration renewal,Runnable active,Clock clock) {
        Execution {
            Objects.requireNonNull(node); Objects.requireNonNull(authorization); Objects.requireNonNull(ownership);
            Objects.requireNonNull(renewal); Objects.requireNonNull(active); Objects.requireNonNull(clock);
            if (renewal.isNegative() || renewal.isZero()) throw new IllegalArgumentException("capture renewal must be positive");
        }
    }
    @FunctionalInterface interface Ports {
        CapturePort open(ConfiguredCaptureRun original,CaptureConfig config,Runnable requireActive,
                io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer preparation,
                io.tapstate.adapters.pdk.PdkCapturePort.StreamPreparer stream);
    }
    private final Stores stores;
    private final Execution execution;
    private final NativeSrsCaptureRunUnit runtime;
    private final SnapshotBuffer buffer;
    private final ConnectorProvisioner connectors;
    private final Ports ports;
    private final java.util.function.LongSupplier reportClock;
    private static final long REPORT_DRAIN_NANOS=Duration.ofSeconds(15).toNanos();
    private Function<String,Optional<SourceBindingSnapshot>> sourceIdentities=id->Optional.empty();
    private Function<String,Optional<SourceBindingSnapshot>> retainedSourceIdentities = id -> sourceIdentities.apply(id);
    @FunctionalInterface interface Operations {SourceOperationObserver open(SourceBindingSnapshot identity,String domain,Runnable guard,Runnable local);}
    private Operations operations=(identity,domain,guard,local)->SourceOperationObserver.NONE;
    ScopedPipelineCaptureCoordinator withSourceOperations(Function<String,Optional<SourceBindingSnapshot>> identities,
            Operations observers){
        this.sourceIdentities=Objects.requireNonNull(identities);this.operations=Objects.requireNonNull(observers);return this;
    }
    ScopedPipelineCaptureCoordinator withRetainedSourceIdentities(Function<String,Optional<SourceBindingSnapshot>> identities) {
        retainedSourceIdentities = Objects.requireNonNull(identities);
        return this;
    }

    private CapturePort observedPort(SourceBindingSnapshot identity,ConfiguredCaptureRun notes,CaptureConfig config,
            String domain,Runnable guard,Runnable local,io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer snapshot,
            io.tapstate.adapters.pdk.PdkCapturePort.StreamPreparer stream,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure){
        CapturePort port=ports.open(notes,config,guard,snapshot,stream);
        if (port instanceof io.tapstate.adapters.pdk.PdkCapturePort pdk) {
            pdk.withOpeningFailure(openingFailure).withReadRetries(retryPolicies);
            if (identity != null) pdk.withOperationObserver(operations.open(identity,domain,guard,local));
        }
        return port;
    }
    private Function<String, RetryPolicy> retryPolicies = operation -> RetrySettingsStore.DEFAULT.disabled();
    private record DirectRetryKey(PipelineIntentScope scope, String source) {}
    private record DirectRetry(ExecutionFence execution, CaptureReadRetry retry) {}
    private final Map<DirectRetryKey, DirectRetry> directRetries = new ConcurrentHashMap<>();

    ScopedPipelineCaptureCoordinator withReadRetries(Function<String, RetryPolicy> policies) {
        retryPolicies = Objects.requireNonNull(policies);
        return this;
    }

    private final Map<ExecutionFence,Session> sessions=new LinkedHashMap<>();
    // Retiring a run can check its capture while holding a CACHE transaction, without waiting on maintenance.
    private final Set<ExecutionFence> startingOrActiveCaptures = ConcurrentHashMap.newKeySet();
    private final Map<String,Owner> owners=new LinkedHashMap<>();
    private final AtomicReference<Predicate<String>> driving=new AtomicReference<>(id -> true);
    private final AtomicBoolean closed=new AtomicBoolean();
    private final Object hostShutdown = new Object();
    private boolean hostClosing;
    private final Map<SrsConsumerKey,Long> waiting=new HashMap<>();
    private final ScheduledExecutorService maintenance=Executors.newSingleThreadScheduledExecutor(task -> {
        var thread=new Thread(task,"tapstate-native-shared-capture");thread.setDaemon(true);return thread;
    });
    private final ExecutorService ownerOperations = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("tapstate-capture-owner-", 1).factory());

    ScopedPipelineCaptureCoordinator(Stores stores,Execution execution,NativeSrsCaptureRunUnit runtime,
            SnapshotBuffer buffer,ConnectorProvisioner connectors) {
        this(stores,execution,runtime,buffer,connectors,(original,config,guard,preparation,stream) -> {
            var port=original.port(config,stores.notes(),connectors,guard);
            if(preparation!=null)port=port.withSnapshotPreparation(preparation);
            return stream==null?port:port.withStreamPreparation(stream);
        });
    }
    ScopedPipelineCaptureCoordinator(Stores stores,Execution execution,NativeSrsCaptureRunUnit runtime,
            SnapshotBuffer buffer,ConnectorProvisioner connectors,Ports ports) {
        this(stores,execution,runtime,buffer,connectors,ports,System::nanoTime);
    }
    ScopedPipelineCaptureCoordinator(Stores stores,Execution execution,NativeSrsCaptureRunUnit runtime,
            SnapshotBuffer buffer,ConnectorProvisioner connectors,Ports ports,java.util.function.LongSupplier reportClock) {
        this.stores=Objects.requireNonNull(stores); this.execution=Objects.requireNonNull(execution);
        this.runtime=Objects.requireNonNull(runtime); this.buffer=Objects.requireNonNull(buffer);
        this.connectors=Objects.requireNonNull(connectors); this.ports=Objects.requireNonNull(ports);
        this.reportClock=Objects.requireNonNull(reportClock);
        maintenance.scheduleWithFixedDelay(this::maintenanceTick,1,1,TimeUnit.SECONDS);
    }
    private PipelineConnectorExecutions connectorExecutions;

    ScopedPipelineCaptureCoordinator withConnectorExecutions(PipelineConnectorExecutions bindings) {
        connectorExecutions = bindings;
        return this;
    }

    @Override public void runOnlyWhileDriving(Predicate<String> driving) { this.driving.set(Objects.requireNonNull(driving)); }

    @Override
    public synchronized SourceModelSnapshot retainedPlanningModels(
            PipelineConvergencePass pass, ArtifactStore artifacts, SourceModelSnapshot discovered, RetainedStartCheck check) {
        active();
        var scope = pass.scope();
        pass.requirePipeline(scope.pipelineId());
        if (!business(scope).equals(stores.scope())) throw refused();
        if (check.hasPreparedModels(() -> {
            active();
            pass.requirePipeline(scope.pipelineId());
        })) return discovered;
        var pipeline =
                PipelineInlining.inline(
                        StoredArtifacts.requirePipeline(artifacts, scope.pipelineId()), artifacts);
        var result = discovered;
        for (var reference : pipeline.sources()) {
            if (reference instanceof SourceRef.Spec spec && Boolean.TRUE.equals(spec.srs())) {
                result =
                        retainedPlanningModel(
                                pass, artifacts, discovered, result, pipeline, reference);
            }
        }
        active();
        pass.requirePipeline(scope.pipelineId());
        return result;
    }

    private SourceModelSnapshot retainedPlanningModel(
            PipelineConvergencePass pass,
            ArtifactStore artifacts,
            SourceModelSnapshot discovered,
            SourceModelSnapshot result,
            PipelineResource pipeline,
            SourceRef reference) {
        var source = StoredArtifacts.requireSource(artifacts, reference.id());
        if (!source.connector().equals("mongodb")) return result;
        var selected =
                SourceCaptureResolution.forPipeline(pipeline, reference, source, discovered.model(source));
        if (selected.isEmpty()) return result;
        var scope = pass.scope();
        var canonical = selected.orElseThrow().chainId().value();
        var held = connectorExecutions == null ? null : connectorExecutions.effective(scope,
                () -> pass.requirePipeline(scope.pipelineId())).binding(reference.binding(), PipelineNode.Purpose.CAPTURE);
        var prepared = stores.configuration().read(stores.scope(),
                held == null ? SourceCaptureIdentity.sharedCapture(canonical) : held.captureId());
        if (prepared.isEmpty()) return result;
        var original = prepared.orElseThrow();
        requirePlanningPreparation(original, canonical, io.tapstate.core.model.PipelineSourceConfig.effective(reference, source));
        var notes = stores.notes().guardedBy(() -> { active(); pass.requirePipeline(scope.pipelineId()); });
        var token = notes.open(PipelineConnectorExecutions.chainOwner(scope, original.canonicalChainId()));
        var origin = held == null ? stores.configuration().inspectSharedOrigin(stores.scope(), reference.binding(),
                io.tapstate.core.model.canonical.CanonicalHash.of(source), pipeline.id(),
                io.tapstate.core.model.canonical.CanonicalHash.of(StoredArtifacts.requirePipeline(artifacts, pipeline.id())))
                .orElseThrow(() -> new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null)) : notes.origin(token);
        if (held != null && !held.sha256().equals(origin.artifactHash())) throw refused();
        var artifact = connectors.resolveRetained(notes, token, origin);
        if (!origin.artifactHash().equals(artifact.contentHash())
                || !origin.equals(
                        io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(
                                source.connector(), origin.artifactHash(), io.tapstate.core.model.PipelineSourceConfig.settings(reference, source))))
            throw refused();
        if (io.tapstate.adapters.pdk.PdkSourceModelComparison.forVerifiedArtifact(
                        source.connector(), artifact)
                != SourceModelComparison.SAMPLED_DOCUMENT) return result;
        var srs =
                stores.sources()
                        .srs(
                                stores.scope(),
                                () -> {
                                    active();
                                    pass.requirePipeline(scope.pipelineId());
                                });
        var context =
                new SrsConsumerContext(
                        new SrsConsumerKey(original.chainId(), scope.pipelineId(), reference.binding()),
                        scope.stateGeneration());
        var membership = srs.consumers().find(context.key());
        if (membership.isEmpty()
                || membership.orElseThrow().detached()
                || !membership.orElseThrow().context().equals(context)) return result;
        var progress =
                srs.consumers()
                        .progress(context)
                        .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        if (progress.membershipRevision() != membership.orElseThrow().revision()) throw refused();
        if (progress.snapshot() == null) return result;
        return result.withRetainedTables(
                reference.binding(), source, retainedTables(srs, progress.snapshot(), original, scope));
    }

    private void requirePlanningPreparation(
            CapturePreparation original, String canonical, SourceResource source) {
        if (!original.scope().equals(stores.scope())
                || original.mode() != CapturePreparation.Mode.SHARED_SRS
                || !original.captureId().equals(SourceCaptureIdentity.sharedCapture(original.canonicalChainId()))
                || !original.canonicalChainId().startsWith("mcv-") && !original.canonicalChainId().equals(canonical)
                || !original.sourceIdentityHash()
                        .equals(
                                SourceCaptureIdentity.physical(
                                        source.connector(), source.config()))) throw refused();
    }

    private Map<String, SourceTable> retainedTables(
            SrsStores srs,
            SrsSnapshotPin snapshot,
            CapturePreparation original,
            PipelineIntentScope scope) {
        var tables = new LinkedHashMap<String, SourceTable>();
        for (var entry : snapshot.models().entrySet()) {
            var key = entry.getValue();
            var model = srs.models().require(key);
            if (!key.equals(model.key())
                    || !key.miningChainId().equals(original.chainId())
                    || !key.cacheIncarnation().equals(scope.cacheIncarnation())
                    || !key.table().equals(entry.getKey())
                    || !model.table().name().equals(entry.getKey())
                    || !model.ringName()
                            .equals(SrsRingbuffer.ringName(original.chainId(), entry.getKey())))
                throw refused();
            tables.put(entry.getKey(), model.table());
        }
        return tables;
    }

    @Override public PipelineCaptureCoordinator forRun(PipelineIntentScope commandScope,WorkloadClaimFence command,ExecutionFence original) {
        requireRun(commandScope,command,original);
        return new Run(commandScope,command,original);
    }
    boolean hasCaptureForRetirement(PipelineIntentScope commandScope, WorkloadClaimFence command, ExecutionFence original) {
        requireRun(commandScope,command,original);
        return startingOrActiveCaptures.contains(original);
    }
    private void requireRun(PipelineIntentScope commandScope,WorkloadClaimFence command,ExecutionFence original) {
        active(); Objects.requireNonNull(commandScope); Objects.requireNonNull(command); Objects.requireNonNull(original);
        if (original.scope()==null || !business(commandScope).equals(stores.scope()) || !business(original.scope()).equals(stores.scope())
                || !commandScope.pipelineId().equals(original.pipelineId()) || !command.key().clusterId().equals(commandScope.clusterId())
                || command.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION || !command.key().resourceId().equals(commandScope.pipelineId())) throw refused();
    }
    @Override public void startCapture(String pipeline) { throw refused(); }
    @Override public void stopCapture(String pipeline,boolean purge) { throw refused(); }

    void retireScope(PipelineIntentScope scope, WorkloadClaimFence ceiling) {
        List<Session> captured;
        synchronized (this) {
            active();
            captured = sessions.values().stream().filter(session -> session.original.scope().equals(scope)).toList();
            if (ceiling == null && !captured.isEmpty()) throw refused();
            for (var session : captured) requireNotLater(session.original, ceiling);
        }
        for (var session : captured) stop(session);
    }
    boolean hasPipelineWork(String pipeline) {
        active();
        return startingOrActiveCaptures.stream().anyMatch(original -> original.pipelineId().equals(pipeline));
    }
    synchronized boolean hasScope(PipelineIntentScope scope) {
        active(); return sessions.keySet().stream().anyMatch(original -> original.scope().equals(scope));
    }
    /** Releases only the original local shared owner after all of its Pipeline sessions have ended. */
    void stopSharedLocally(CapturePreparation original, WorkloadClaimFence fence) {
        requireShared(original, fence);
        // The separate all-member verification still proves the durable end.
        synchronized (this) {
            if (!owners.containsKey(original.captureId())) return;
        }
        retireShared(original, fence);
    }

    void retireShared(CapturePreparation original, WorkloadClaimFence fence) {
        requireShared(original, fence);
        Owner owner;
        CompletableFuture<Void> retirement;
        for (;;) {
            CompletableFuture<Void> preceding;
            synchronized (this) {
                owner = owners.get(original.captureId());
                if (owner == null) {
                    retirement = null;
                    break;
                }
                if (!original.equals(owner.prepared) || !fence.equals(owner.fence) || !owner.sessions.isEmpty()) throw refused();
                preceding = owner.operationAttempt;
                if (preceding == null) {
                    retirement = reserveOwnerOperation(owner);
                    break;
                }
            }
            waitForOwner(preceding);
        }
        if (owner == null) {
            execution.ownership().requireEnded(fence);
            return;
        }
        try {
            if (!owner.ended.get() && !owner.drainingReports() && needed(owner)) throw refused();
            owner.finishCheckpointingForRetirement();
            owner.ended.set(true);
            owner.release();
            execution.ownership().endTenure(fence);
            synchronized (this) { owners.remove(original.captureId(), owner); }
        } finally { finishOwnerOperation(owner, retirement); }
    }

    void requireSharedRetired(CapturePreparation original, WorkloadClaimFence fence) {
        requireShared(original, fence);
        synchronized (this) {
            if (owners.containsKey(original.captureId())) throw refused();
        }
        execution.ownership().requireEnded(fence);
    }
    private void requireShared(CapturePreparation original,WorkloadClaimFence fence) {
        active();
        if(original==null || original.mode()!=CapturePreparation.Mode.SHARED_SRS || !stores.scope().equals(original.scope())
                || fence==null || fence.key().type()!=WorkloadClaimType.CAPTURE || !original.scope().clusterId().equals(fence.key().clusterId())
                || !original.captureId().equals(fence.key().resourceId()))throw refused();
    }
    static void requireNotLater(ExecutionFence original, WorkloadClaimFence ceiling) {
        if (!original.pipelineId().equals(ceiling.key().resourceId()) || original.scope() == null
                || !original.scope().clusterId().equals(ceiling.key().clusterId())
                || ceiling.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || original.claimGeneration() > ceiling.claimGeneration() || original.executionGeneration() > ceiling.executionGeneration()
                || original.topologyRevision() > ceiling.topologyRevision()
                || original.claimGeneration() == ceiling.claimGeneration() && (!original.ownerNodeId().equals(ceiling.owner().nodeId())
                        || !original.ownerBootId().equals(ceiling.owner().bootId()))) throw refused();
    }

    private final class Run implements PipelineCaptureCoordinator {
        private final PipelineIntentScope commandScope;
        private final WorkloadClaimFence command;
        private final ExecutionFence original;
        private final AtomicReference<io.tapstate.core.common.FailureFact> openingFailure = new AtomicReference<>();
        Run(PipelineIntentScope commandScope,WorkloadClaimFence command,ExecutionFence original) {
            this.commandScope=commandScope; this.command=command; this.original=original;
        }
        @Override public void startCapture(String pipeline) { throw refused(); }
        @Override public void startCapture(String pipeline, ArtifactStore captured, SourceModelSnapshot models) {
            prepareCaptureStart(pipeline, captured, models).run();
        }
        @Override public Runnable prepareCaptureStart(String pipeline, ArtifactStore captured, SourceModelSnapshot models) {
            requireId(pipeline);
            if (!original.scope().equals(commandScope) || !claim(original).equals(command)) throw refused();
            var inputs = captureInputs(original, captured, models);
            return () -> start(this, inputs);
        }
        @Override public io.tapstate.core.common.FailureFact openingFailureFact(String pipeline, Throwable failure) {
            requireId(pipeline);
            var fact = openingFailure.getAndSet(null);
            return fact != null && fact.cause() == failure ? fact : new io.tapstate.core.common.FailureFact(failure);
        }
        @Override public void stopCapture(String pipeline, boolean purge) {
            stopCaptureIfPresent(pipeline, purge);
        }
        @Override public boolean stopCaptureIfPresent(String pipeline, boolean purge) {
            requireId(pipeline);
            if (purge) throw refused();
            Session session;
            synchronized (ScopedPipelineCaptureCoordinator.this) { session = sessions.get(original); }
            if (session == null) return false;
            stop(session);
            return true;
        }
        @Override public PipelineCaptureBindings captureBindings() { return session().bindings; }
        @Override public ValidationCaptureRoute validationRoute(String alias,String table) {
            synchronized(ScopedPipelineCaptureCoordinator.this){return route(session(),alias,table);}
        }
        @Override public boolean isCapturing(String pipeline) { requireId(pipeline); synchronized(ScopedPipelineCaptureCoordinator.this) { return sessions.containsKey(original); } }
        @Override public Optional<Throwable> captureFailure(String pipeline) {
            return captureFailureFact(pipeline).map(io.tapstate.core.common.FailureFact::cause);
        }
        @Override public Optional<io.tapstate.core.common.FailureFact> captureFailureFact(String pipeline) {
            requireId(pipeline); synchronized(ScopedPipelineCaptureCoordinator.this) {
                var held=sessions.get(original); if(held==null)return Optional.empty();
                if(held.failure.get()!=null)return Optional.of(held.failure.get());
                for (var source : held.sources.values()) {
                    var physical = source.owner == null ? null : source.owner.physical;
                    if (physical == null) continue;
                    var failure = physical.failureFact();
                    if (failure.isPresent()) return failure;
                }
                return held.sources.values().stream().filter(source -> source.run!=null).map(source -> source.run.health().failureFact()).flatMap(Optional::stream).findFirst();
            }
        }
        @Override public boolean loadDelivered(String pipeline) {
            requireId(pipeline); var held=current();
            if(held==null || !held.snapshot || held.sources.isEmpty())return true;
            return PipelineDiagnostics.measure(pipeline, Phase.PIPELINE_TARGET_CONFIRM, () -> {
                requireSession(held);
                boolean delivered = held.bounded ? stores.boundedLoads().complete(original)
                        : held.sources.values().stream().allMatch(source ->
                                streamingCursor(held,source).snapshotCompletedTables().containsAll(source.selected.tables()));
                if (delivered) PipelineDiagnostics.event("initial_snapshot_delivery_observed");
                return delivered;
            });
        }
        @Override public SnapshotReading snapshotProgress(String pipeline) { requireId(pipeline); var held=current(); return held==null?SnapshotReading.NONE:snapshotReading(held); }
        @Override public io.tapstate.runtime.scheduler.CaptureMeasurements measurements(String pipeline,
                ExecutionPlan plan, Instant at) {
            requireId(pipeline);
            var held = current();
            return held == null ? io.tapstate.runtime.scheduler.CaptureMeasurements.NONE : captureMeasurements(held, plan, at, true, true);
        }
        @Override public SnapshotReading runSnapshotProgress(String pipeline) { requireId(pipeline); var held=current(); return held==null?SnapshotReading.NONE:runSnapshotReading(held); }
        @Override public InitialLoad initialLoad(String pipeline, ExecutionPlan plan, Instant at) {
            requireId(pipeline); var held=current(); return held==null?null:initialLoadReading(held,plan,at);
        }
        private Session current() { synchronized(ScopedPipelineCaptureCoordinator.this) { return sessions.get(original); } }
        private Session session() {
            synchronized (ScopedPipelineCaptureCoordinator.this) {
                var session = sessions.get(original);
                if (session == null || session.starting || session.bindings == null) throw refused();
                return session;
            }
        }
        private void requireId(String pipeline) { if(!original.pipelineId().equals(pipeline))throw refused(); }
    }
    /** Exact original session only; a current claim never relabels an old alias binding. */
    synchronized ValidationCaptureRoute validationRoute(ExecutionFence original,String alias,String table) {
        var held=sessions.get(original);if(held==null)throw refused();return route(held,alias,table);
    }
    private ValidationCaptureRoute route(Session held,String alias,String table) {
        if (held.starting || held.bindings == null) throw refused();
        requireSession(held);var source=held.sources.get(alias);
        if(held.bounded||source==null||!source.selected.tables().contains(table))throw refused();
        var state=stores.preparations().control(source.prepared,table);
        if(!source.prepared.equals(state.preparation()))throw refused();
        var expected=source.owner==null?source.foreign:source.owner.fence;
        if(!state.claim().equals(expected))throw refused();
        execution.authorization().requireCapture(stores.scope(),expected);
        String channel;long epoch,membership;
        if(source.shared){
            if(!source.srs.consumers().attached(source.consumer,table)||!state.servesTable())throw refused();
            channel=source.prepared.chainId();epoch=state.epoch();membership=source.consumer.membershipRevision();
        }else{
            var fixed=held.bindings.direct().get(alias);if(fixed==null)throw refused();
            var direct=source.channels.control(fixed.context(),table).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if(direct.epoch()!=fixed.epoch()||!direct.capture().equals(fixed.captureClaim())||!direct.pipeline().equals(fixed.pipelineClaim())||!direct.selectedTable())throw refused();
            if(!fixed.captureClaim().equals(expected)||!fixed.tables().contains(table))throw refused();
            channel=fixed.context().channelId();epoch=fixed.epoch();membership=0;
        }
        requireSession(held);execution.authorization().requireCapture(stores.scope(),expected);
        return new ValidationCaptureRoute(held.original,alias,table,state.preparation(),expected,channel,epoch,membership);
    }
    private void start(Run run, CaptureInputs inputs) {
        Session failed = null;
        Throwable failure = null;
        synchronized (this) {
            if (hostClosing) throw refused();
            boolean alreadyPresent = sessions.containsKey(run.original);
            startingOrActiveCaptures.add(run.original);
            try {
                startOwned(run, inputs);
            } catch (RuntimeException | Error refused) {
                failure = refused;
                if (!alreadyPresent) {
                    failed = sessions.get(run.original);
                    if (failed != null) failed.ended.set(true);
                }
            } finally {
                var original = sessions.get(run.original);
                if (original == null) startingOrActiveCaptures.remove(run.original);
                else if (!alreadyPresent) original.starting = false;
            }
        }
        if (failed != null) {
            Session original = failed;
            failure = cleanup(failure, () -> stop(original));
        }
        rethrow(failure);
    }
    private void forgetSession(Session session) {
        if (sessions.remove(session.original, session)) startingOrActiveCaptures.remove(session.original);
    }
    private record CaptureInputs(ArtifactStore artifacts, SourceModelSnapshot models, PipelineResource authored,
            PipelineResource pipeline, Map<String, SourceResource> sources, PipelinePreparationInputs preparation) {}

    private CaptureInputs captureInputs(ExecutionFence original, ArtifactStore artifacts, SourceModelSnapshot models) {
        active();
        Objects.requireNonNull(artifacts);
        Objects.requireNonNull(models).requirePipeline(original.pipelineId());
        var authored = StoredArtifacts.requirePipeline(artifacts, original.pipelineId());
        var pipeline = PipelineInlining.inline(authored, artifacts);
        var sources = new LinkedHashMap<String, SourceResource>();
        for (String id : pipeline.sourceIds()) sources.put(id, StoredArtifacts.requireSource(artifacts, id));
        var prepared = models.preparationInputs(original.scope(), authored, pipeline, sources);
        return new CaptureInputs(artifacts, models, authored, pipeline, Collections.unmodifiableMap(sources), prepared);
    }

    private void startOwned(Run run, CaptureInputs inputs) {
        active();
        requirePipeline(run.original);
        var existing = sessions.get(run.original);
        if (existing != null) {
            if (existing.starting || existing.stopping || existing.ended.get() || !existing.inputs.equals(inputs.preparation())) throw refused();
            return;
        }
        run.openingFailure.set(null);
        var pipeline = inputs.pipeline();
        var mode =
                pipeline.settings() == null || pipeline.settings().readMode() == null
                        ? ReadMode.SNAPSHOT_AND_CDC
                        : pipeline.settings().readMode();
        var session = new Session(run.original, inputs.preparation(), execution.clock().instant(), mode, run.openingFailure);
        session.replaysKeyedCopies = replayableKeyedCopy(pipeline);
        if (connectorExecutions != null)
            session.selection = connectorExecutions.selection(run.original.scope(), () -> requirePipeline(run.original));
        sessions.put(run.original, session);
        var node = execution.node().get().orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        if (!node.owner().equals(claim(run.original).owner())) throw refused();
        prepareSources(session, pipeline, inputs.sources(), inputs.models(), node);
        startSources(session, pipeline, node, inputs.artifacts(), inputs.authored());
    }

    private void prepareSources(
            Session session,
            PipelineResource pipeline,
            Map<String, SourceResource> sourceDefinitions,
            SourceModelSnapshot models,
            WorkloadClaimFence node) {
        for (String binding : pipeline.sourceBindings()) {
            var reference = pipeline.sourceReference(binding);
            prepareSource(session, pipeline, reference, sourceDefinitions.get(reference.id()), models, node);
        }
    }

    private void prepareSource(
            Session session,
            PipelineResource pipeline,
            SourceRef reference,
            SourceResource definition,
            SourceModelSnapshot models,
            WorkloadClaimFence node) {
        var frozenModel = models.modelForBinding(reference.binding(), definition);
        var selected = SourceCaptureResolution.forPipeline(pipeline, reference, definition, frozenModel);
        if (selected.isEmpty()) return;
        boolean shared = !session.bounded && reference instanceof SourceRef.Spec spec && Boolean.TRUE.equals(spec.srs());
        var binding = session.selection.binding(reference.binding(), PipelineNode.Purpose.CAPTURE);
        if (binding == null && shared && connectorExecutions != null && stores.recoveryPinned().test(session.original.scope())) {
            var selectedSource = selected.orElseThrow();
            binding = connectorExecutions.recoveryBinding(session.original.scope(),
                    new PipelineConnectorExecutions.Node(reference.binding(), PipelineNode.Purpose.CAPTURE,
                            definition.connector(), io.tapstate.core.model.PipelineSourceConfig.settings(reference, definition), selectedSource.chainId().value()),
                    session.inputs.sourceHashes().get(reference.binding()), session.inputs.pipelineHash(), () -> requireSession(session));
        }
        var previous = session.selection.previous(reference.binding(), PipelineNode.Purpose.CAPTURE);
        if (shared && previous != null && binding == null) throw refused();
        var opportunity = binding == null || binding.captureId() == null ? null
                : stores.configuration().read(stores.scope(), binding.captureId()).orElse(null);
        boolean switching = previous != null && shared && !binding.chainId().equals(previous.chainId());
        var artifact = binding == null || !shared || !binding.notes().owner().ownerId().startsWith("mcv-")
                || opportunity != null && !switching ? null
                : new CapturePreparationRequest.SharedArtifact(binding.sha256(),
                        switching ? session.selection.intentRevision() : null);
        var request = new CapturePreparationRequest(session.original.scope(), claim(session.original),
                node, reference.binding(), session.inputs, artifact);
        CaptureCreationResult issued;
        if (opportunity != null && !switching) issued = new CaptureCreationResult.AlreadyIssued(opportunity);
        else if (shared && binding == null && stores.sources().supportsAdmittedCaptureStartup())
            issued = stores.configuration().issueForStartup(request, selected.orElseThrow().chainId().value());
        else issued = stores.configuration().issueFirstCreate(request);
        var fixed = issued instanceof CaptureCreationResult.Issued first
                ? first.creation().preparation() : ((CaptureCreationResult.AlreadyIssued) issued).preparation();
        if (shared && binding == null && !selected.orElseThrow().chainId().value().equals(fixed.canonicalChainId()))
            throw refused();
        var prepared =
                issued instanceof CaptureCreationResult.Issued first
                        ? stores.preparations().createFirst(first.creation())
                        : stores.preparations()
                                .load(((CaptureCreationResult.AlreadyIssued) issued).preparation());
        if (prepared.preparation().mode()
                != (session.bounded
                        ? CapturePreparation.Mode.SNAPSHOT_ONLY
                        : shared
                                ? CapturePreparation.Mode.SHARED_SRS
                                : CapturePreparation.Mode.DIRECT)) throw refused();
        var resolution =
                shared
                        ? selected.orElseThrow()
                                .onPhysicalChain(
                                        selected.orElseThrow().chainId().value(),
                                        prepared.preparation().chainId())
                        : selected.orElseThrow();
        var source = new Source(reference.binding(), resolution, shared, frozenModel, reference.id());
        source.canonicalChain = selected.orElseThrow().chainId().value();
        source.artifact = binding;
        source.previousArtifact = previous;
        if (shared && binding != null && (!binding.captureId().equals(prepared.preparation().captureId())
                || !binding.chainId().equals(prepared.preparation().chainId())))
            throw refused();
        session.sources.put(source.id, source);
        boolean retained = issued instanceof CaptureCreationResult.AlreadyIssued;
        var identities = shared && retained ? retainedSourceIdentities : sourceIdentities;
        source.identity = identities.apply(definition.id())
                .filter(identity -> identity.contentHash().equals(session.inputs.sourceHashes().get(source.id))).orElse(null);
        source.prepared = prepared.preparation();
        source.owner = owner(session, source, prepared,true);
        if (shared) {
            if (source.owner != null && binding != null && source.owner.artifact == null) {
                source.owner.artifact = binding;
                source.owner.previousArtifact = previous;
            }
            attachSharedSource(session, source, issued instanceof CaptureCreationResult.Issued first
                    && first.creation().newSharedHistory(), retained);
        }
    }

    private void attachSharedSource(Session session, Source source, boolean newHistory, boolean retained) {
        if (!newHistory) {
            var history = stores.configuration().inspectSharedHistory(
                    source.prepared, source.id, session.inputs.sourceHashes().get(source.id),
                    session.original.pipelineId(), session.inputs.pipelineHash());
            if (!source.prepared.equals(history.preparation())) throw refused();
            source.positionEditsRestricted = history.positionEditsRestricted();
        }
        if (source.owner != null)
            source.owner.positionEditsRestricted |= source.positionEditsRestricted;
        source.srs = stores.sources().srs(stores.scope(), () -> requireSession(session));
        var key =
                new SrsConsumerKey(
                        source.prepared.chainId(), session.original.pipelineId(), source.id);
        var context = new SrsConsumerContext(key, session.original.scope().stateGeneration());
        if (source.previousArtifact != null && !source.previousArtifact.chainId().equals(context.key().miningChainId())) {
            var oldKey = new SrsConsumerKey(source.previousArtifact.chainId(), session.original.pipelineId(), source.id);
            var oldContext = new SrsConsumerContext(oldKey, session.original.scope().stateGeneration());
            source.handoff = source.srs.consumers().progress(oldContext)
                    .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if (source.owner == null) throw refused();
            var replacement = source.srs.consumers().prepareConnectorHandoff(source.handoff, context,
                    source.owner.fence, claim(session.original), session.began)
                    .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            source.consumer = SrsConsumerRun.of(session.original.scope(), replacement);
            source.owner.handoff = new PendingCaptureHandoff(source.handoff, context, claim(session.original));
            return;
        }
        if (newHistory) {
            var first = source.srs.consumers().attach(context, 0, Set.copyOf(source.selected.tables()), claim(session.original));
            if (first.isPresent()) {
                source.consumer = SrsConsumerRun.of(session.original.scope(), first.orElseThrow());
                return;
            }
        }
        var tables = Set.copyOf(source.selected.tables());
        var fence = claim(session.original);
        var consumers = source.srs.consumers();
        Optional<SrsConsumerMembership> attached;
        source.retainedStartup = retained && source.owner != null && !source.positionEditsRestricted
                && stores.sources().supportsAdmittedCaptureStartup();
        if (source.retainedStartup && source.owner.physical == null && session.inputs.sourceHashes().size() == 1) {
            var prepared = consumers.attachAndPrepareForStartup(context, tables, fence);
            source.startupPreparation = prepared.flatMap(SrsConsumerStore.StartupAttachment::preparation).orElse(null);
            attached = prepared.map(SrsConsumerStore.StartupAttachment::member);
        } else if (source.retainedStartup) {
            attached = consumers.attachForStartup(context, tables, fence);
        } else {
            var held = consumers.find(key);
            attached = held.filter(member -> !member.detached() && member.context().equals(context)
                    && member.tables().equals(tables) && member.commandFence().equals(fence))
                    .or(() -> consumers.attach(context, held.map(SrsConsumerMembership::revision).orElse(0L), tables, fence));
        }
        var relation = attached.orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        source.consumer = SrsConsumerRun.of(session.original.scope(), relation);
    }

    private record SnapshotBindings(
            long epoch, Map<String, PipelineCaptureBindings.SnapshotOnlySource> sources) {}

    private SnapshotBindings snapshotBindings(Session session, SnapshotStateStore state) {
        var snapshots = new LinkedHashMap<String, PipelineCaptureBindings.SnapshotOnlySource>();
        boolean retained = !session.sources.isEmpty() && session.sources.values().stream().allMatch(source -> source.retainedStartup);
        long prior = state == null ? 0 : retained ? state.startupOrder() : state.order();
        long epoch = 0;
        if (session.bounded && !session.sources.isEmpty()) {
            var captures = new LinkedHashMap<String, CaptureCacheState>();
            session.sources.forEach(
                    (id, source) ->
                            captures.put(
                                    id,
                                    new CaptureCacheState(
                                            source.owner.prepared,
                                            source.owner.fence,
                                            Optional.empty())));
            epoch =
                    Objects.requireNonNull(state, "bounded capture requires snapshot state")
                            .advanceOrder(prior, prior, captures);
            for (var source : session.sources.values())
                snapshots.put(
                        source.id,
                        new PipelineCaptureBindings.SnapshotOnlySource(
                                source.selected.chainId().value(),
                                Set.copyOf(source.selected.tables()),
                                epoch));
        }
        return new SnapshotBindings(epoch, snapshots);
    }

    private Map<String, DirectSourceRun> prepareDirectSources(
            Session session, PipelineResource pipeline) {
        var direct = new LinkedHashMap<String, DirectSourceRun>();
        if (session.bounded) return direct;
        for (var source : session.sources.values()) {
            if (source.shared) continue;
            Runnable guard =
                    () -> {
                        requireSession(session);
                        source.owner.require();
                    };
            var preparation =
                    session.inputs.directChannels().stream()
                            .filter(value -> value.context().sourceId().equals(source.id))
                            .findFirst()
                            .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if (!preparation.context().captureId().equals(source.owner.prepared.captureId()))
                throw refused();
            source.channels = stores.sources().directChannels(stores.scope(), guard);
            var before = source.channels.load(preparation.context());
            long allocated =
                    source.channels
                            .beginCapture(
                                    preparation.context(),
                                    before.revision(),
                                    before.epoch(),
                                    source.owner.fence,
                                    claim(session.original))
                            .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            source.direct = source.channels.load(preparation.context());
            if (source.direct.epoch() != allocated) throw refused();
            var binding = DirectSourceRun.of(session.original, source.direct);
            direct.put(source.id, binding);
            String token = source.direct.resumeToken();
            source.start =
                    token == null
                            ? sourceStart(pipeline)
                            : CaptureStart.resume(new SourcePosition(token));
        }
        return direct;
    }

    private void startSources(
            Session session,
            PipelineResource pipeline,
            WorkloadClaimFence node,
            ArtifactStore artifacts,
            PipelineResource authored) {
        var state =
                session.snapshot
                        ? stores.pipelines()
                                .snapshotStates(
                                        session.original.scope(),
                                        claim(session.original),
                                        () -> requireSession(session))
                        : null;
        var snapshots = snapshotBindings(session, state);
        var direct = prepareDirectSources(session, pipeline);
        var logged = new LinkedHashMap<String, PipelineCaptureBindings.LoggedSource>();
        var verifiedChains = new HashSet<String>();
        for (var source : session.sources.values()) {
            if (source.shared)
                PipelineDiagnostics.measure(session.original.pipelineId(), Phase.CAPTURE_SOURCE_PREPARE, () -> {
                    PipelineDiagnostics.source(source.id);
                    return prepareLoggedSource(session, source, pipeline, node, artifacts, authored,
                            session.snapshot && verifiedChains.add(source.prepared.chainId()));
                })
                        .ifPresent(binding -> logged.put(source.id, binding));
        }
        // The preparation mode belongs to the state generation; its READY receipt suppresses
        // repeated target clearing.
        session.bindings =
                new PipelineCaptureBindings(
                        session.original,
                        logged,
                        direct,
                        snapshots.sources(),
                        session.snapshot && !session.sources.isEmpty());
        for (var source : session.sources.values()) {
            source.snapshotEpoch = snapshots.epoch();
            source.readTables = Set.copyOf(source.selected.tables());
        }
        for (var source : session.sources.values()) {
            startSource(session, source, state, snapshots.epoch(), logged, direct);
        }
        var sampled =
                session.sources.values().stream()
                        .filter(source -> source.sampled)
                        .map(source -> source.id)
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
        session.bindings =
                new PipelineCaptureBindings(
                        session.original,
                        logged,
                        direct,
                        snapshots.sources(),
                        session.snapshot && !session.sources.isEmpty(),
                        sampled.isEmpty() ? null : session.inputs,
                        sampled);
        requireSession(session);
    }

    private Optional<PipelineCaptureBindings.LoggedSource> prepareLoggedSource(
            Session session,
            Source source,
            PipelineResource pipeline,
            WorkloadClaimFence node,
            ArtifactStore artifacts,
            PipelineResource authored,
            boolean verifyControlClosure) {
        var consumers = source.srs.consumers();
        var context = source.consumer.context();
        boolean admitted = stores.sources().supportsAdmittedCaptureStartup();
        SrsChainState before;
        SrsConsumerProgress retained;
        SrsConsumerStore.AttachedChain startupTail = null;
        if (admitted && source.retainedStartup && source.owner != null && source.owner.physical == null) {
            var preparation = source.startupPreparation;
            source.startupPreparation = null;
            if (preparation == null)
                preparation = consumers.startupPreparation(context, verifyControlClosure)
                        .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            var sampled = preparation.snapshot();
            before = sampled.chain();
            retained = sampled.progress();
            startupTail = new SrsConsumerStore.AttachedChain(preparation.topology(), before);
        } else if (admitted) {
            var sampled = consumers.startupSnapshot(context, Map.of(), verifyControlClosure)
                    .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            before = sampled.chain();
            retained = sampled.progress();
        } else {
            before = source.srs.chains().load(source.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            retained = consumers.progress(context).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        }
        requireStartWindow(session, source, pipeline, before, retained);
        ensureSharedTail(session, source, before, retained, node, artifacts, authored, startupTail);
        if (source.handoff != null) finishArtifactHandoff(session, source,
                source.srs.chains().load(source.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused));
        if (source.freshFull != null) {
            source.srs.chains().load(source.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            waiting.remove(context.key());
            return Optional.empty();
        }

        var rings = new TreeMap<String, String>();
        source.selected.tables().forEach(table -> rings.put(table, source.selected.ringName(table)));
        SrsChainState chain;
        SrsConsumerProgress progress;
        if (admitted) {
            // Tail startup can change the epoch or publish new heads. Sample again after it returns.
            var sampled = consumers.startupSnapshot(context, rings).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            chain = sampled.chain();
            requirePublishedTables(source, chain, sampled.published());
            var start = session.snapshot ? StartFrom.latest() : readStart(pipeline);
            var bounds = chain.pendingFreshFull() == null && !(start instanceof StartFrom.At)
                    ? Optional.of(sampled.bounds()) : Optional.<Map<String, SrsLogBounds>>empty();
            progress = stores.sources().withCaptureStartup(stores.scope(),
                    () -> prepareInitialArrivals(session, source, chain, start, sampled.progress(), bounds, true));
        } else {
            chain = source.srs.chains().load(source.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if (chain.epoch() == 0 || !chain.servingTables().containsAll(rings.keySet())) throw awaiting(source);
            requirePublishedTables(source, chain, source.srs.models().publishedForNewSnapshot(source.prepared.chainId(), rings));
            var start = session.snapshot ? StartFrom.latest() : readStart(pipeline);
            progress = prepareInitialArrivals(session, source, chain, start,
                    consumers.progress(context).orElseThrow(ScopedPipelineCaptureCoordinator::refused), Optional.empty(), false);
        }
        if (source.freshFull == null && session.snapshot
                && !progress.cursor().snapshotCompletedTables().containsAll(source.selected.tables()))
            source.retainedSnapshot = progress;
        waiting.remove(source.consumer.context().key());
        long originalEpoch =
                progress.snapshot() != null
                        ? progress.snapshot().snapshotEpoch()
                        : chain.pendingFreshFull() != null
                                ? chain.pendingFreshFull().snapshotEpoch()
                                : chain.epoch();
        return Optional.of(
                new PipelineCaptureBindings.LoggedSource(
                        source.consumer,
                        originalEpoch,
                        progress.ringDoneThrough(),
                        source.canonicalChain));
    }

    private void finishArtifactHandoff(Session session, Source source, SrsChainState prepared) {
        var context = source.consumer.context();
        var observed = source.srs.consumers().progress(context).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        if (observed.cursor().sinkAcked() != null) return;
        // A decoded starting token alone does not prove the connector could open and read its stream.
        var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        SrsChainState ready = prepared;
        while (ready.checkpoint() == null
                || ready.checkpoint().position().order().seq() == io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ) {
            requireSession(session);
            var failure = source.owner.physical.tail.health().failure();
            failure.ifPresent(ScopedPipelineCaptureCoordinator::throwFailure);
            if (System.nanoTime() - deadline >= 0)
                throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", prepared.miningChainId()), null);
            pauseForArtifactProof();
            ready = source.srs.chains().load(prepared.miningChainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        }
        var rings = new LinkedHashMap<String, String>();
        source.selected.tables().forEach(table -> rings.put(table, source.selected.ringName(table)));
        var models = source.srs.models().publishedForNewSnapshot(ready.miningChainId(), rings)
                .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        var keys = new LinkedHashMap<String, SrsModelKey>();
        models.forEach((table, model) -> keys.put(table, model.key()));
        var pin = new SrsSnapshotPin(ready.epoch(), source.handoff.cursor().sinkAcked().token(), keys);
        if (!source.srs.consumers().completeConnectorHandoff(source.handoff, context,
                observed.membershipRevision(), observed.revision(), pin, claim(session.original))) throw refused();
    }

    private static void pauseForArtifactProof() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException stop) {
            Thread.currentThread().interrupt();
            throw refused();
        }
    }

    private SrsConsumerProgress prepareInitialArrivals(Session session, Source source, SrsChainState chain,
            StartFrom start, SrsConsumerProgress progress, Optional<Map<String, SrsLogBounds>> bounds, boolean admitted) {
        var consumers = source.srs.consumers();
        var context = source.consumer.context();
        requireSession(session);
        var arrivals = initialArrivals(session, source, chain, progress, start, bounds);
        if (arrivals.isEmpty()) return progress;
        long memberRevision = source.consumer.membershipRevision();
        var original = claim(session.original);
        if (admitted) {
            return consumers.startReadingAfterAndGet(context, memberRevision, progress.revision(), arrivals, original)
                    .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        }
        if (!consumers.startReadingAfter(context, memberRevision, progress.revision(), arrivals, original)) throw refused();
        return consumers.progress(context).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
    }

    private void requirePublishedTables(Source source, SrsChainState chain,
            Optional<Map<String, SrsModel>> published) {
        if (chain.epoch() == 0 || !chain.servingTables().containsAll(source.selected.tables())) throw awaiting(source);
        if (published.isEmpty()
                || published.orElseThrow().values().stream()
                        .anyMatch(model -> model.key().captureEpoch() != chain.epoch()))
            throw awaiting(source);
    }

    private void requireStartWindow(
            Session session,
            Source source,
            PipelineResource pipeline,
            SrsChainState before,
            SrsConsumerProgress retained) {
        if (!session.snapshot
                && readStart(pipeline) instanceof StartFrom.At at
                && at.instant().isBefore(execution.clock().instant())
                && !retained.ringDoneThrough().keySet().containsAll(source.selected.tables())
                && (before.epoch() == 0
                        || !before.servingTables().containsAll(source.selected.tables()))) {
            throw new TapstateException(
                    CaptureError.START_FROM_OUTSIDE_WINDOW,
                    Map.of(
                            "requested",
                            at.instant().toString(),
                            "earliest",
                            execution.clock().instant().toString(),
                            "retention",
                            before.retention() == null ? "unset" : before.retention()),
                    null);
        }
    }

    private void ensureSharedTail(
            Session session,
            Source source,
            SrsChainState before,
            SrsConsumerProgress retained,
            WorkloadClaimFence node,
            ArtifactStore artifacts,
            PipelineResource authored, SrsConsumerStore.AttachedChain startupTail) {
        Owner originalOwner = source.owner;
        if (originalOwner == null) return;
        bindOpener(originalOwner, session, source);
        try {
            if (stores.sources().supportsAdmittedCaptureStartup()) {
                var opened = stores.sources().withCaptureStartup(stores.scope(), () -> ensureTail(source.owner, true, startupTail, session.openingFailure::set));
                if (opened != null && session.snapshot && session.original.equals(opened.original()))
                    source.tailOpening = opened;
            } else ensureTail(source.owner, false, session.openingFailure::set);
        } catch (SrsBaselineModelMismatch mismatch) {
            var failedTail = originalOwner.physical;
            if (failedTail != null) {
                awaitMismatchClose(originalOwner, () -> closeFailedTail(originalOwner, failedTail, mismatch), mismatch);
            }
            if (!session.snapshot
                    || hostClosing
                    || sessions.get(session.original) != session
                    || source.owner != originalOwner
                    || owners.get(originalOwner.prepared.captureId()) != originalOwner
                    || retained.snapshot() != null
                    || before.pendingFreshFull() != null
                    || !source.prepared.chainId().equals(mismatch.chainId())
                    || source.owner.physical != null
                    || mismatch.getSuppressed().length != 0) throw mismatch;
            requireSession(session);
            originalOwner.require();
            var full =
                    stores.completedFull()
                            .find(
                                    session.original.scope(),
                                    session.inputs,
                                    PipelineInputClosure.dependencyHashes(
                                            artifacts, authored, session.inputs))
                            .orElseThrow(() -> mismatch);
            source.freshFull =
                    source.srs
                            .consumers()
                            .inspectFreshFull(
                                    full,
                                    source.consumer,
                                    retained.revision(),
                                    node,
                                    source.owner.fence);
        }
    }

    private Map<String, Long> initialArrivals(
            Session session,
            Source source,
            SrsChainState chain,
            SrsConsumerProgress progress,
            StartFrom start,
            Optional<Map<String, SrsLogBounds>> bounds) {
        var arrivals = new LinkedHashMap<String, Long>();
        for (String table : source.selected.tables()) {
            if (progress.ringDoneThrough().containsKey(table)) continue;
            if (bounds.isEmpty()) {
                arrivals.put(table, initialReadAfter(session, source, chain, table, start));
            } else {
                var boundary = bounds.orElseThrow().get(source.selected.ringName(table));
                arrivals.put(table, start instanceof StartFrom.Latest ? boundary.largestSequence() : boundary.trimmedThrough());
            }
        }
        return arrivals;
    }

    private long initialReadAfter(
            Session session, Source source, SrsChainState chain, String table, StartFrom start) {
        if (chain.pendingFreshFull() != null) {
            var key = chain.pendingFreshFull().models().get(table);
            if (key == null) throw refused();
            var fixed = source.srs.models().require(key);
            if (!fixed.ringName().equals(source.selected.ringName(table))) throw refused();
            return fixed.ddlSequence() - 1;
        }
        return runtime.initialReadAfter(
                stores.scope(),
                source.prepared.chainId(),
                table,
                start,
                chain.retention(),
                () -> requireSession(session));
    }

    private void startSource(
            Session session, Source source, SnapshotStateStore state, long epoch,
            Map<String, PipelineCaptureBindings.LoggedSource> logged, Map<String, DirectSourceRun> direct) {
        PipelineDiagnostics.measure(session.original.pipelineId(), Phase.CAPTURE_SOURCE_START, () -> {
            PipelineDiagnostics.source(source.id);
            startSourceOwned(session, source, state, epoch, logged, direct);
            return null;
        });
    }

    private void startSourceOwned(
            Session session,
            Source source,
            SnapshotStateStore state,
            long epoch,
            Map<String, PipelineCaptureBindings.LoggedSource> logged,
            Map<String, DirectSourceRun> direct) {
        var originalHandoffs = sourceHandoffs(session, source);
        Owner originalOwner = source.owner;
        Runnable guard =
                source.shared
                        ? () -> requireSession(session)
                        : () -> {
                            requireSession(session);
                            originalOwner.require();
                        };
        source.guard = guard;
        if (source.shared
                && !session.snapshot
                && !source.selected.config().connectorId().equals("mongodb")) return;
        var config =
                source.selected
                        .config()
                        .at(new PipelineNode(session.original.pipelineId(), source.id));
        if (source.shared)
            config = config.sharing(new SharedNotes(source.prepared.canonicalChainId(), List.of()));
        var fixedConfig = config;
        var notes = source.tailOpening;
        source.tailOpening = null;
        if (notes == null || source.owner.opener == null || !config.equals(source.owner.opener.config())) {
            boolean recovery = stores.recoveryPinned().test(session.original.scope());
            notes = source.artifact != null
                    ? ConfiguredCaptureRun.prepareBound(session.original, config, stores.notes(), connectors,
                            guard, source.artifact, source.previousArtifact,
                            source.shared && session.snapshot && stores.sources().supportsAdmittedCaptureStartup() && !recovery)
                    : source.shared && session.snapshot && stores.sources().supportsAdmittedCaptureStartup() && !recovery
                        ? ConfiguredCaptureRun.prepareStartup(session.original, config, stores.notes(), connectors, guard)
                        : ConfiguredCaptureRun.prepare(session.original, config, stores.notes(), connectors, guard, recovery);
        }
        var comparison = notes.preparedModelComparison(config, stores.notes(), connectors, guard);
        source.sampled = comparison == SourceModelComparison.SAMPLED_DOCUMENT;
        if (source.shared && !session.snapshot) return;
        CaptureHandoff output = snapshotOutput(session, source, state, originalHandoffs, guard);
        if (source.shared) {
            startSharedSnapshot(session, source, logged, notes, fixedConfig, output, guard, comparison);
        } else {
            startPrivateSource(session, source, config, notes, output, direct, epoch);
            if (!session.bounded && source.artifact != null && source.previousArtifact != null
                    && !source.artifact.sha256().equals(source.previousArtifact.sha256()))
                requirePrivateHandoff(session, source);
        }
        guard.run();
    }

    private void requirePrivateHandoff(Session session, Source source) {
        var health = source.run.health();
        var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (!health.receivedSourcePosition()) {
            requireSession(session);
            health.failure().ifPresent(ScopedPipelineCaptureCoordinator::throwFailure);
            if (System.nanoTime() - deadline >= 0)
                throw new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE,
                        Map.of("channel", source.direct.context().channelId()), null);
            pauseForArtifactProof();
        }
        health.failure().ifPresent(ScopedPipelineCaptureCoordinator::throwFailure);
    }

    private static boolean replayableKeyedCopy(PipelineResource pipeline) {
        return (pipeline.transforms() == null || pipeline.transforms().isEmpty())
                && pipeline.view() == null
                && pipeline.serve() instanceof ServeBlock.Inline serve
                && (serve.push() == null || serve.push().isEmpty())
                && serve.sync() != null && !serve.sync().isEmpty()
                && serve.sync().stream().allMatch(sink -> sink.rename() == null
                        && sink.writeMode() != io.tapstate.core.model.WriteMode.APPEND);
    }

    private static CapturePort snapshotReplay(Session session, Source source, CapturePort port) {
        if (session.replaysKeyedCopies && source.expectedModels != null
                && port instanceof io.tapstate.adapters.pdk.PdkCapturePort pdk) {
            pdk.withReplayableSnapshotTables(table -> {
                var model = source.expectedModels.get(table);
                return model != null && !model.primaryKey().isEmpty();
            });
        }
        return port;
    }

    private CapturePort directPort(Session session, Source source, CapturePort port) {
        snapshotReplay(session, source, port);
        if (session.bounded || !(port instanceof io.tapstate.adapters.pdk.PdkCapturePort pdk)) return port;
        var key = new DirectRetryKey(session.original.scope(), source.id);
        pdk.withStreamRecovery(failure -> {
            if (!session.ended.get()) directRetries.compute(key, (ignored, existing) -> {
                if (session.ended.get() || existing != null
                        && existing.execution().executionGeneration() > session.original.executionGeneration()) return existing;
                var episode = existing != null && existing.execution().equals(session.original)
                        ? existing : new DirectRetry(session.original, new CaptureReadRetry());
                episode.retry().interrupted(failure);
                return episode;
            });
        }, () -> directRetries.computeIfPresent(key, (ignored, episode) ->
                    episode.execution().equals(session.original) ? null : episode),
                () -> {
                    var episode = directRetries.get(key);
                    return episode != null && episode.execution().equals(session.original) && episode.retry().inProgress();
                });
        return pdk;
    }

    Optional<CaptureReadRetry> directRetry(ExecutionFence original) {
        return directRetries.entrySet().stream()
                .filter(entry -> entry.getValue().execution().equals(original) && entry.getValue().retry().waiting())
                .sorted(Map.Entry.comparingByKey(java.util.Comparator.comparing(DirectRetryKey::source)))
                .map(entry -> entry.getValue().retry()).findFirst();
    }

    boolean hasDirectRetryFor(ExecutionFence original, Throwable failure) {
        return directRetries.values().stream().anyMatch(episode ->
                episode.execution().equals(original) && episode.retry().waitingFor(failure));
    }

    RetryPolicy streamRetryPolicy() {
        return retryPolicies.apply(RetrySettingsStore.STREAM_READ);
    }

    void carryDirectRetries(ExecutionFence previous, ExecutionFence replacement) {
        directRetries.replaceAll((key, value) -> value.execution().equals(previous)
                ? new DirectRetry(replacement, value.retry()) : value);
    }

    void clearDirectRetries(PipelineIntentScope scope) {
        directRetries.keySet().removeIf(key -> key.scope().equals(scope));
    }

    private void startPrivateSource(
            Session session,
            Source source,
            CaptureConfig config,
            ConfiguredCaptureRun notes,
            CaptureHandoff output,
            Map<String, DirectSourceRun> direct,
            long epoch) {
        var guard = source.guard;
        var fixedConfig = config;
        if (!session.bounded && session.snapshot) {
            source.run =
                    runtime.beginDirectSnapshot(
                            config,
                            source.direct,
                            source.channels,
                            preparation -> {
                                source.snapshotEpoch = preparation.request().snapshotEpoch();
                                source.readTables = Set.copyOf(preparation.stillOwed());
                                return directPort(session, source, observedPort(source.identity,notes,fixedConfig,
                                        source.prepared.captureId()+":snapshot:"+session.original.pipelineId()+":"+session.original.scope().stateGeneration(),
                                        guard,()->requireSessionLocal(session),preparation::prepare,null,session.openingFailure::set));
                            },
                            output,
                            buffer.capacity(),
                            guard);
            source.direct = source.channels.load(source.direct.context());
            direct.put(source.id, DirectSourceRun.of(session.original, source.direct));
        } else {
            var port = directPort(session, source, observedPort(source.identity,notes,config,
                    source.prepared.captureId()+":direct:"+session.original.pipelineId()+":"+session.original.scope().stateGeneration(),
                    guard,()->requireSessionLocal(session),null,null,session.openingFailure::set));
            source.run =
                    session.bounded
                            ? runtime.beginSnapshotOnly(config, epoch, port, output, guard)
                            : runtime.startDirect(
                                    config,
                                    source.direct,
                                    source.channels,
                                    source.start,
                                    port,
                                    output,
                                    buffer.capacity(),
                                    guard);
        }
    }

    private Map<String, CaptureHandoff> sourceHandoffs(Session session, Source source) {
        String handoff =
                source.freshFull == null
                        ? session.bindings.bufferKey(source.id)
                        : PipelineCaptureBindings.bufferKey(session.original, source.id);
        var declared = new LinkedHashMap<String, CaptureHandoff>();
        for (String table : source.selected.tables()) {
            String ring = source.selected.ringName(table);
            if (session.snapshot) buffer.declareSnapshot(handoff, ring);
            declared.put(table, buffer.handoff(handoff, ring));
        }
        return Map.copyOf(declared);
    }

    private CaptureHandoff snapshotOutput(
            Session session,
            Source source,
            SnapshotStateStore state,
            Map<String, CaptureHandoff> originalHandoffs,
            Runnable guard) {
        return new CaptureHandoff() {
            @Override
            public void accept(Envelope row) {
                guard.run();
                if (!source.selected.tables().contains(row.src())) throw refused();
                originalHandoffs.get(row.src()).accept(row);
                if (row.op() == Op.READ) source.rows.merge(row.src(), 1L, Long::sum);
            }

            @Override
            public void loaded(String table) {
                guard.run();
                if (!session.snapshot || !source.selected.tables().contains(table)) throw refused();
                if (source.readTables.contains(table))
                    state.recordCount(
                            source.id,
                            table,
                            source.snapshotEpoch,
                            source.rows.getOrDefault(table, 0L));
                guard.run();
                source.completed.add(table);
                originalHandoffs.get(table).loaded(table);
            }
        };
    }

    private void startSharedSnapshot(
            Session session,
            Source source,
            Map<String, PipelineCaptureBindings.LoggedSource> logged,
            ConfiguredCaptureRun notes,
            CaptureConfig fixedConfig,
            CaptureHandoff output,
            Runnable guard,
            SourceModelComparison startupComparison) {
        var binding = logged.get(source.id);
        long firstEpoch =
                source.freshFull == null
                        ? binding.snapshotEpoch()
                        : source.freshFull.originalChain().epoch();
        Function<SrsSnapshotPreparation, CapturePort> prepareSnapshot =
                preparation -> {
                    source.snapshotEpoch = preparation.request().snapshotEpoch();
                    source.readTables = Set.copyOf(preparation.stillOwed());
                    return snapshotReplay(session, source, observedPort(
                            source.identity,notes,
                            fixedConfig,
                            source.prepared.captureId()+":snapshot:"+session.original.pipelineId()+":"+session.original.scope().stateGeneration(),
                            guard,()->requireSessionLocal(session),
                            (request, seam, actual) -> {
                                var comparison = notes.startupArtifact() == null
                                        ? notes.modelComparison(fixedConfig, stores.notes(), connectors, guard)
                                        : startupComparison;
                                requireSnapshotModels(source, actual, comparison);
                                preparation.prepare(request, seam, actual, comparison);
                            },
                            null, session.openingFailure::set));
                };
        var retained = source.retainedSnapshot;
        source.retainedSnapshot = null;
        if (retained != null)
            source.run = runtime.resumeSnapshot(source.consumer, fixedConfig, firstEpoch, prepareSnapshot, output, guard, retained);
        else if (source.freshFull == null)
            source.run =
                    runtime.beginSnapshot(
                            source.consumer,
                            fixedConfig,
                            firstEpoch,
                            prepareSnapshot,
                            output,
                            guard);
        else
            source.run =
                    runtime.beginSnapshot(
                            source.consumer,
                            fixedConfig,
                            firstEpoch,
                            prepareSnapshot,
                            output,
                            guard,
                            source.freshFull,
                            session.began,
                            receipt -> {
                                guard.run();
                                startPreparedTail(source.owner, receipt.chain(), source.owner.configured, session.openingFailure::set, false);
                                guard.run();
                                logged.put(
                                        source.id,
                                        new PipelineCaptureBindings.LoggedSource(
                                                source.consumer,
                                                receipt.progress().snapshot().snapshotEpoch(),
                                                receipt.progress().ringDoneThrough(),
                                                source.canonicalChain));
                            });
    }

    private Owner owner(Session session,Source source,CaptureCacheState prepared) { return owner(session,source,prepared,false); }
    private Owner owner(Session session,Source source,CaptureCacheState prepared,boolean initial) {
        var id=prepared.preparation().captureId(); var existing=owners.get(id);
        if(existing!=null) {
            if (existing.stopping || existing.drainingReports()) throw new RingNotOpenYet(new CaptureId(id));
            if(existing.ended.get())throw refused();
            if(!existing.prepared.equals(prepared.preparation()))throw refused(); source.owner=existing; existing.require();
            if (!existing.lease.retain(() -> {
                existing.sessions.add(session.original);
                existing.demandRevision++;
                existing.demand.set(true);
            })) throw refused();
            return existing;
        }
        var permit=initial?execution.ownership().acquireInitial(new CaptureId(id)):execution.ownership().acquire(new CaptureId(id));
        if(!permit.acquired()) {
            if(!source.shared)throw new RingNotOpenYet(new CaptureId(id));
            source.foreign=stores.preparations().load(prepared.preparation()).claim();return null;
        }
        if(permit.fence()==null)throw refused();
        var owner=new Owner(prepared.preparation(),permit.claim()); owners.put(id,owner); owner.sessions.add(session.original); source.owner=owner;
        owner.identity=source.identity;
        var parent = execution.ownership().parentNode();
        owner.lease = parent == null ? new CaptureClaimLease(execution.ownership(), permit.claim(), execution.renewal())
                : CaptureClaimLease.followingParent(execution.ownership(), permit.claim(), execution.renewal(), parent,
                        execution.node(), () -> execution.authorization().requireCapture(stores.scope(), owner.fence));
        owner.lease.onLost(() -> lost(owner));
        owner.lease.whileNeeded(owner.demand::get);
        owner.require(); return owner;
    }
    /** The physical capture keeps one opener identity for its entire ownership tenure. */
    private record CaptureOpener(ExecutionFence original, CaptureConfig config) {}

    private void bindOpener(Owner owner, Session session, Source source) {
        if (owner.opener != null) return;
        var config = source.selected.config().at(new PipelineNode(session.original.pipelineId(), source.id))
                .sharing(new SharedNotes(source.prepared.canonicalChainId(), List.of()));
        owner.opener = new CaptureOpener(session.original, config);
        owner.origin = owner.opener.original();
        owner.config = owner.opener.config();
    }

    private record PendingCaptureHandoff(SrsConsumerProgress original, SrsConsumerContext candidate,
            WorkloadClaimFence pipeline) {}

    private final class Owner {
        final CapturePreparation prepared; final WorkloadClaimFence fence; final WorkloadClaim initial;
        final CaptureReadRetry retry = new CaptureReadRetry();
        final Set<ExecutionFence> sessions=ConcurrentHashMap.newKeySet(); final AtomicBoolean ended=new AtomicBoolean();
        SourceBindingSnapshot identity;CaptureOpener opener;CaptureClaimLease lease;ExecutionFence origin;CaptureConfig config;ConfiguredCaptureRun configured;boolean positionEditsRestricted;
        // The owner operation alone replaces the physical handle; observers may read it during maintenance.
        @SuppressWarnings("java:S3077")
        volatile Physical physical;
        ConnectorRunBindings.Binding artifact, previousArtifact;
        PendingCaptureHandoff handoff;
        SrsStores controlStores;
        final AtomicBoolean demand = new AtomicBoolean(true);
        // Guarded by the coordinator monitor; an attachment invalidates an outstanding demand read.
        long demandRevision;
        long nextDemandCheck = System.nanoTime() + execution.renewal().toNanos();
        volatile Long reportDrainStarted;
        volatile boolean handingOff;
        // Guarded by the coordinator monitor while physical retirement runs without it.
        boolean stopping;
        CompletableFuture<Void> operationAttempt;
        boolean maintenanceRetirementPending;
        Owner(CapturePreparation prepared,WorkloadClaim initial) { this.prepared=prepared;this.initial=initial;this.fence=WorkloadClaimFence.from(initial); }
        void finishCheckpointingForRetirement() {
            if (ended.get() || drainingReports() || physical == null || !physical.live.get()) return;
            try {
                physical.finishCheckpointing();
            } catch (TapstateException missing) {
                if (missing.code() != IoError.SRS_MODEL_UNAVAILABLE) throw missing;
                // Control identity has already been proved. An unprovable pending cut must not keep the owner alive.
                LOG.warn("Retiring capture {} at its last durable checkpoint [{}]", prepared.captureId(), missing.code().code());
            }
        }
        void release() {
            if(physical!=null) {physical.close();physical=null;}
            if(lease!=null)lease.closeAfterPhysicalExit();else execution.ownership().release(initial);
        }
        void require() {
            active();
            if (ended.get()) throw refused();
            if (!fence.equals(execution.authorization().requireCapture(stores.scope(), fence))
                    || ended.get()) throw refused();
        }
        boolean drainingReports(){return reportDrainStarted!=null;}
        boolean reportTimeRemaining(){long elapsed=reportClock.getAsLong()-reportDrainStarted;return elapsed>=0&&elapsed<REPORT_DRAIN_NANOS;}
    }
    private static final class Source {
        boolean retainedStartup;
        Runnable guard;boolean sampled;SourceBindingSnapshot identity;
        final String id;
        final String sourceId;
        final SourceCaptureResolution selected;final boolean shared;boolean positionEditsRestricted; final Map<String,Long> rows=new ConcurrentHashMap<>();
        final Set<String> completed=ConcurrentHashMap.newKeySet();
        final Map<String,SourceTable> expectedModels;
        final Map<String,Long> estimates = new LinkedHashMap<>();
        Owner owner;CapturePreparation prepared;WorkloadClaimFence foreign;SrsStores srs;SrsConsumerRun consumer;SrsFreshFullPreparation freshFull; CaptureRun run; DirectChannelStore channels; DirectChannelProgress direct; CaptureStart start; long snapshotEpoch; Set<String> readTables=Set.of();
        ConfiguredCaptureRun tailOpening;
        SrsConsumerStore.StartupPreparation startupPreparation;
        SrsConsumerProgress retainedSnapshot;

        String canonicalChain;
        ConnectorRunBindings.Binding artifact, previousArtifact;
        SrsConsumerProgress handoff;
        Source(String id,SourceCaptureResolution selected,boolean shared,SourceModel frozen,String sourceId) {
            this.id=id;this.sourceId=sourceId;this.selected=selected;this.shared=shared;
            if(frozen==null)expectedModels=null;
            else {
                var fixed=new LinkedHashMap<String,SourceTable>();
                for(var table:frozen.tables())if(selected.tables().contains(table.name())) {
                    if(fixed.put(table.name(),new SourceTable(table.name(),table.fields(),table.primaryKey(),table.indexes()))!=null)
                        throw modelUnproven(this);
                    if (table.approximateRowCount() != null) estimates.put(table.name(), table.approximateRowCount());
                }
                expectedModels=Map.copyOf(fixed);
            }
        }
    }
    private static void requireSnapshotModels(Source source,Map<String,SourceTable> actual,SourceModelComparison comparison) {
        if(source.expectedModels==null) {if(source.freshFull!=null)throw modelUnproven(source);return;}
        for(var entry:actual.entrySet()) {
            var table=entry.getValue();
            var expected=source.expectedModels.get(entry.getKey());
            if(!entry.getKey().equals(table.name()) || expected==null
                    || !comparison.acceptsObservation(expected,table))throw modelUnproven(source);
        }
    }
    private static TapstateException modelUnproven(Source source) {
        return new TapstateException(IoError.SRS_FULL_PREPARATION_UNPROVEN,Map.of("chain",source.selected.chainId().value()),null);
    }
    private static final class Session {
        final ExecutionFence original; final PipelinePreparationInputs inputs; final Instant began; final ReadMode mode; final boolean bounded,snapshot;
        final Map<String,Source> sources=new LinkedHashMap<>(); final AtomicBoolean ended=new AtomicBoolean(); final AtomicReference<io.tapstate.core.common.FailureFact> failure=new AtomicReference<>();
        PipelineCaptureBindings bindings;
        boolean starting = true;
        boolean stopping;
        PipelineConnectorExecutions.Selection selection = PipelineConnectorExecutions.Selection.EMPTY;
        boolean replaysKeyedCopies;
        final AtomicReference<io.tapstate.core.common.FailureFact> openingFailure;
        Session(ExecutionFence original,PipelinePreparationInputs inputs,Instant began,ReadMode mode,AtomicReference<io.tapstate.core.common.FailureFact> openingFailure) { this.openingFailure=openingFailure; this.original=original;this.inputs=inputs;this.began=began;this.mode=mode;this.bounded=mode==ReadMode.SNAPSHOT_ONLY;this.snapshot=mode!=ReadMode.CDC_ONLY; }
        String label(String source,String table) {
            return PipelineTableSelection.label(source, table, sources.values().stream().map(item -> item.selected.tables()));
        }
    }
    record ObservationSelection(ExecutionFence execution, Map<SrsConsumerRun, io.tapstate.adapters.mongostore.MongoCacheDomain.ObservationSource> sources,
            Map<String, CapturePreparation> preparations) {}

    synchronized Optional<ObservationSelection> observationSelection(ExecutionFence original) {
        var held = sessions.get(original);
        if (held == null || held.starting || !held.snapshot || held.bounded || held.sources.isEmpty() || held.bindings == null
                || held.sources.values().stream().anyMatch(source -> !source.shared || source.prepared == null
                        || source.consumer == null || source.snapshotEpoch < 1)) return Optional.empty();
        if (!observationSessionCurrent(held)) return Optional.empty();
        var tables = new LinkedHashMap<SrsConsumerRun, io.tapstate.adapters.mongostore.MongoCacheDomain.ObservationSource>();
        var preparations = new LinkedHashMap<String, CapturePreparation>();
        for (var source : held.sources.values()) {
            tables.put(source.consumer, new io.tapstate.adapters.mongostore.MongoCacheDomain.ObservationSource(
                    source.sourceId, source.snapshotEpoch, Set.copyOf(source.selected.tables())));
            preparations.put(source.id, source.prepared);
        }
        return Optional.of(new ObservationSelection(original, Map.copyOf(tables), Map.copyOf(preparations)));
    }

    synchronized Optional<io.tapstate.runtime.scheduler.ObservationPublisher.CaptureSample> observationSample(
            ObservationSelection original, io.tapstate.adapters.mongostore.MongoCacheDomain.ObservationFacts facts,
            ExecutionPlan plan, Instant at) {
        var held = sessions.get(original.execution());
        if (held == null || plan == null || !original.equals(observationSelection(held.original).orElse(null))
                || !held.original.pipelineId().equals(plan.pipelineId())
                || !Objects.equals(plan.claimGeneration(), held.original.claimGeneration())
                || !Objects.equals(plan.executionGeneration(), held.original.executionGeneration())
                || !Objects.equals(plan.topologyRevision(), held.original.topologyRevision())) return Optional.empty();
        var loaded = new LinkedHashMap<String, TableSnapshot>();
        var readThisRun = new LinkedHashMap<String, TableSnapshot>();
        var stages = new LinkedHashMap<String, InitialLoad.Table>();
        var positions = new LinkedHashMap<String, String>();
        for (var source : held.sources.values()) {
            var cursor = facts.cursors().get(source.id);
            if (cursor == null || !cursor.snapshotCompletedTables().containsAll(source.selected.tables())
                    || !source.completed.containsAll(source.selected.tables())) return Optional.empty();
            for (String table : source.selected.tables()) {
                String label = held.label(source.id, table);
                var saved = facts.counts().getOrDefault(source.id, Map.of()).get(table);
                if (saved != null) loaded.put(label, new TableSnapshot(saved, saved, 100, true));
                long currentRows = source.rows.getOrDefault(table, 0L);
                readThisRun.put(label, new TableSnapshot(currentRows, currentRows, 100, true));
                Long rows = source.rows.get(table);
                if (rows == null && source.readTables.contains(table)) rows = 0L;
                stages.put(label, new InitialLoad.Table(InitialLoad.Stage.LANDED, rows, source.estimates.get(table)));
                var ack = cursor.sinkAckedByTable().get(table);
                if (ack != null && ack.token() != null) positions.put(label, ack.token());
            }
        }
        if (!observationSessionCurrent(held)) return Optional.empty();
        return Optional.of(new io.tapstate.runtime.scheduler.ObservationPublisher.CaptureSample(
                new SnapshotReading(loaded, held.began), new SnapshotReading(readThisRun, held.began), positions,
                new InitialLoad(InitialLoad.executionKey(plan), at, held.mode.name().toLowerCase(Locale.ROOT), stages)));
    }

    private boolean observationSessionCurrent(Session session) {
        active();
        if (session.ended.get()) throw refused();
        try {
            execution.authorization().requireLocal(session.original);
            return true;
        } catch (TapstateException unavailable) {
            if (unavailable.code() != EngineError.EXECUTION_NOT_AUTHORIZED) throw unavailable;
            return false;
        }
    }

    private io.tapstate.runtime.scheduler.CaptureMeasurements captureMeasurements(Session session, ExecutionPlan plan, Instant at,
            boolean snapshotCounts, boolean includePositions) {
        requireSession(session);
        var original = session.original;
        boolean matchesPlan = plan != null && original.pipelineId().equals(plan.pipelineId())
                && Objects.equals(plan.claimGeneration(), original.claimGeneration())
                && Objects.equals(plan.executionGeneration(), original.executionGeneration())
                && Objects.equals(plan.topologyRevision(), original.topologyRevision());
        var state = session.snapshot && snapshotCounts ? stores.pipelines().snapshotStates(original.scope(), claim(original),
                () -> requireSession(session)) : null;
        boolean boundedLanded = session.snapshot && session.bounded && stores.boundedLoads().complete(original);
        var loaded = new LinkedHashMap<String, TableSnapshot>();
        var readThisRun = new LinkedHashMap<String, TableSnapshot>();
        var load = new LinkedHashMap<String, InitialLoad.Table>();
        var positions = new LinkedHashMap<String, String>();
        for (var source : session.sources.values()) {
            // The transaction validates this exact consumer, membership and epoch once. Every projection
            // uses the resulting immutable offset; no proof is retained for a subsequent publication.
            var cursor = session.bounded || !session.snapshot && !includePositions ? null : streamingCursor(session, source);
            if (includePositions && cursor != null) {
                var confirmedPositions = new LinkedHashMap<String, String>();
                StoreBackedSinkPositions.addConfirmedPositions(confirmedPositions, source.selected.tables(), cursor);
                confirmedPositions.forEach((table, position) -> positions.put(session.label(source.id, table), position));
            }
            if (!session.snapshot) continue;
            var confirmed = cursor == null ? List.<String>of() : cursor.snapshotCompletedTables();
            var rows = Map.copyOf(source.rows);
            var completed = Set.copyOf(source.completed);
            for (String table : source.selected.tables()) {
                String label = session.label(source.id, table);
                boolean landed = boundedLanded || confirmed.contains(table);
                if (snapshotCounts && completed.contains(table)) {
                    var count = state.count(source.id, table, source.snapshotEpoch);
                    if (count.isPresent()) loaded.put(label, new TableSnapshot(count.getAsLong(),
                            landed ? count.getAsLong() : null, landed ? 100 : null, landed));
                }
                long read = rows.getOrDefault(table, 0L);
                readThisRun.put(label, new TableSnapshot(read, landed ? read : null, landed ? 100 : null, landed));
                Long readRows = rows.get(table);
                if (readRows == null && completed.contains(table) && source.readTables.contains(table)) readRows = 0L;
                var stage = landed ? InitialLoad.Stage.LANDED : completed.contains(table) ? InitialLoad.Stage.LANDING
                        : readRows != null ? InitialLoad.Stage.READING : InitialLoad.Stage.PENDING;
                load.put(label, new InitialLoad.Table(stage, readRows, source.estimates.get(table)));
            }
        }
        requireSession(session);
        return new io.tapstate.runtime.scheduler.CaptureMeasurements(
                session.snapshot ? new SnapshotReading(loaded, session.began) : SnapshotReading.NONE,
                session.snapshot ? new SnapshotReading(readThisRun, session.began) : SnapshotReading.NONE,
                matchesPlan ? new InitialLoad(InitialLoad.executionKey(plan), at, session.mode.name().toLowerCase(Locale.ROOT), load) : null,
                session.bounded ? Optional.empty() : Optional.of(positions));
    }

    private SnapshotReading snapshotReading(Session session) {
        return captureMeasurements(session, null, execution.clock().instant(), true, false).snapshot();
    }

    private InitialLoad initialLoadReading(Session session, ExecutionPlan plan, Instant at) {
        requireSession(session);
        var original = session.original;
        if (plan == null || !original.pipelineId().equals(plan.pipelineId())
                || !Objects.equals(plan.claimGeneration(), original.claimGeneration())
                || !Objects.equals(plan.executionGeneration(), original.executionGeneration())
                || !Objects.equals(plan.topologyRevision(), original.topologyRevision())) return null;
        return captureMeasurements(session, plan, at, false, false).initialLoad();
    }

    private SnapshotReading runSnapshotReading(Session session) {
        return captureMeasurements(session, null, execution.clock().instant(), false, false).runSnapshot();
    }

    private ConsumerOffset streamingCursor(Session session,Source source) {
        ConsumerOffset cursor;
        if(source.shared) {
            var progress=source.srs.consumers().progress(source.consumer.context()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if(!progress.context().equals(source.consumer.context()) || progress.membershipRevision()!=source.consumer.membershipRevision())throw refused();
            cursor=progress.cursor();
        } else {
            var progress=source.channels.load(source.direct.context());
            session.bindings.direct().get(source.id).requireOriginal(progress);cursor=progress.cursor();
        }
        if(session.snapshot && cursor.snapshotEpoch()!=source.snapshotEpoch)throw refused();
        return cursor;
    }
    private final class Physical {
        final AtomicBoolean live=new AtomicBoolean(true),reportsLive=new AtomicBoolean(true);
        final Owner owner;
        // Opening publishes this reference once; the tail owns its mutable lifecycle.
        @SuppressWarnings("java:S3077")
        volatile NativeSrsCaptureRunUnit.Tail tail;
        final Runnable guard = new NativeSrsCaptureRunUnit.RetainedTailOpening() {
            @Override public void run() { require(); }
            @Override public boolean retainFailedTail(Throwable failure) {
                return Thread.holdsLock(ScopedPipelineCaptureCoordinator.this);
            }
        };
        SourceOperationObserver reports=SourceOperationObserver.NONE;
        volatile boolean ioExitConfirmed;
        Physical(Owner owner){this.owner=owner;}
        Optional<io.tapstate.core.common.FailureFact> failureFact() {
            var opened = tail;
            return opened == null ? Optional.empty() : opened.health().failureFact();
        }
        void require(){owner.require();if(!live.get())throw refused();}
        void finishCheckpointing() { if (tail != null) tail.finishCheckpointing(); }
        void requireLocal(){active();if(!live.get() || owner.ended.get())throw refused();execution.authorization().requireCaptureLocal(stores.scope(),owner.fence);}
        void requireReport(){owner.require();if(!reportLive())throw refused();}
        void requireReportLocal(){active();if(owner.ended.get()||!reportLive())throw refused();execution.authorization().requireCaptureLocal(stores.scope(),owner.fence);}
        boolean reportLive(){return reportsLive.get()&&(!owner.drainingReports()||owner.reportTimeRemaining());}
        void closeIo(){
            live.set(false);reports.close();if(ioExitConfirmed)return;
            if(tail!=null)tail.close();runtime.stopOpenings(guard);ioExitConfirmed=true;
        }
        void close(){reportsLive.set(false);closeIo();}
    }
    private boolean needed(Owner owner) {
        // Report expiry does not prove that an interrupted or failed physical close has finished.
        var physical = owner.physical;
        if(physical!=null && !physical.live.get() && !physical.ioExitConfirmed)return true;
        if(closed.get())return false;
        if(owner.drainingReports())return owner.reportTimeRemaining()&&physical!=null&&physical.reports.hasPendingReports();
        if(owner.sessions.stream().anyMatch(original -> driving.get().test(original.pipelineId())
                && execution.authorization().authorized(original)))return true;
        if(owner.prepared.mode()!=CapturePreparation.Mode.SHARED_SRS)return false;
        owner.require();
        for(var member:retainedConsumers(owner)) {
            if(member.commandFence().owner().equals(owner.initial.owner()))continue;
            var scope=new PipelineIntentScope(stores.scope().clusterId(),stores.scope().bindingVersion(),stores.scope().cacheIncarnation(),member.context().key().pipelineId(),member.context().stateGeneration());
            var original=new ExecutionFence(scope,member.commandFence());
            if(!execution.authorization().authorized(original))continue;
            var state=stores.pipelines().pipelineStates(scope).read(scope.pipelineId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            var status=StateJson.parse(state.stateJson());if(status==PipelineState.RUNNING || status==PipelineState.PAUSED)return true;
        }
        return false;
    }
    private List<SrsConsumerMembership> retainedConsumers(Owner owner) {
        var consumers=stores.sources().srs(stores.scope(),owner::require).consumers();
        try { return consumers.attached(owner.prepared.chainId()).consumers(); }
        catch(TapstateException unavailable) {
            // Closing the original physical owner needs control identity, never reconstructed model bodies.
            var observed=consumers.inspectSharedRecovery(owner.prepared);
            owner.require();
            if(!owner.prepared.equals(observed.previous()) || !owner.fence.equals(observed.captureFence()))throw refused();
            return observed.consumers();
        }
    }
    private ConfiguredCaptureRun ensureTail(Owner owner, boolean admitted,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure) {
        return ensureTail(owner, admitted, null, openingFailure);
    }

    private ConfiguredCaptureRun ensureTail(Owner owner, boolean admitted, SrsConsumerStore.AttachedChain startupTail,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure) {
        if (owner.retry.waiting()) throw new RingNotOpenYet(new CaptureId(owner.prepared.captureId()));
        return ensureTail(owner, tailStores(owner), admitted, null, openingFailure, true, startupTail);
    }

    private SrsStores tailStores(Owner owner) {
        if(owner.drainingReports())throw new RingNotOpenYet(new CaptureId(owner.prepared.captureId()));
        owner.require();if(owner.config==null || owner.origin==null)throw refused();
        if(owner.controlStores==null)owner.controlStores=stores.sources().srs(stores.scope(),owner::require);
        return owner.controlStores;
    }

    private ConfiguredCaptureRun ensureTail(Owner owner, SrsStores current, boolean admitted,
            SrsConsumerStore.RetentionSnapshot snapshot,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure, boolean recoverMismatch) {
        return ensureTail(owner, current, admitted, snapshot, openingFailure, recoverMismatch, null);
    }

    private ConfiguredCaptureRun ensureTail(Owner owner, SrsStores current, boolean admitted,
            SrsConsumerStore.RetentionSnapshot snapshot,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure, boolean recoverMismatch,
            SrsConsumerStore.AttachedChain startupTail) {
        String chainId = owner.prepared.chainId();
        var consumers = current.consumers();
        var requested = new TreeSet<String>();
        SrsChainState before;
        if (snapshot != null) {
            snapshot.consumers().forEach(consumer -> requested.addAll(consumer.membership().tables()));
            if (requested.isEmpty()) return null;
            before = snapshot.chain();
        } else {
            var attached = startupTail != null ? startupTail : admitted ? consumers.attachedWithChain(chainId) : consumers.supportsControlSnapshot()
                    ? consumers.controlSnapshot(chainId).map(value->new SrsConsumerStore.AttachedChain(value.topology(),value.chain()))
                            .orElseThrow(ScopedPipelineCaptureCoordinator::refused) : consumers.attachedWithChain(chainId, current.chains());
            var topology = attached == null ? consumers.attached(chainId) : attached.topology();
            topology.consumers().forEach(member -> requested.addAll(member.tables()));
            if (requested.isEmpty()) return null;
            before = attached == null ? current.chains().load(chainId).orElseThrow(ScopedPipelineCaptureCoordinator::refused)
                    : attached.chain();
        }
        if(owner.physical!=null && !owner.physical.live.get())throw refused();
        if(owner.physical!=null && before.servingTables().containsAll(requested))return null;
        var configured=owner.configured;
        if (configured == null) {
            boolean recoveryPinned = stores.recoveryPinned().test(owner.opener.original().scope());
            configured = owner.artifact != null
                    ? ConfiguredCaptureRun.prepareBound(owner.opener.original(), owner.opener.config(), stores.notes(), connectors, owner::require,
                            owner.artifact, owner.previousArtifact, admitted && !recoveryPinned)
                    : admitted && !recoveryPinned
                        ? ConfiguredCaptureRun.prepareStartup(owner.opener.original(), owner.opener.config(), stores.notes(), connectors, owner::require)
                        : ConfiguredCaptureRun.prepare(owner.opener.original(), owner.opener.config(), stores.notes(), connectors, owner::require, recoveryPinned);
        } else {
            var notes = stores.notes().guardedBy(owner::require);
            connectors.resolveRetained(notes, configured.primary(), notes.origin(configured.primary()));
            owner.require();
        }
        if(owner.physical!=null){
            owner.physical.finishCheckpointing();
            owner.physical.close();owner.physical=null;
            before=current.chains().load(owner.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        }
        if(before.epoch()>0 && before.checkpoint()==null && before.resumeOverride() == null)throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN,Map.of("chain",owner.prepared.chainId()),null);
        SrsChainState opened;
        if (owner.handoff != null && before.checkpoint() == null) {
            var handoff = owner.handoff;
            opened = current.consumers().reopenConnectorHandoffEpoch(handoff.original(), handoff.candidate(),
                    handoff.pipeline(), owner.fence, before.epoch(), requested).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        } else if (startupTail != null) {
            var published = current.chains().openServingEpochForStartup(chainId, before.epoch(),
                    before.consumerRevision(), requested, owner.fence);
            if (published.isEmpty()) {
                var refreshed = current.consumers().attachedWithChain(chainId);
                if (refreshed.chain().epoch() != before.epoch()) throw refused();
                var refreshedTables = requestedTables(refreshed.topology());
                if (refreshedTables.isEmpty()) return null;
                published = current.chains().openServingEpochForStartup(chainId, refreshed.chain().epoch(),
                        refreshed.chain().consumerRevision(), refreshedTables, owner.fence);
            }
            opened = published.orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        } else if (admitted) {
            opened = current.chains().openServingEpoch(chainId, before.epoch(), requested, owner.fence)
                    .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        } else {
            before = current.chains().requestTables(chainId, requested, owner.fence);
            long epoch = current.chains().openEpoch(chainId, before.epoch(), owner.fence)
                    .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if (!current.chains().publishServingTables(chainId, epoch, before.requestedTables(), owner.fence)) throw refused();
            opened = current.chains().load(chainId).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        }
        owner.configured = configured.retained();
        startPreparedTail(owner, opened, configured, openingFailure, recoverMismatch);
        return configured.startupArtifact() == null ? null : configured;
    }

    private static Set<String> requestedTables(SrsConsumerTopology topology) {
        var requested = new TreeSet<String>();
        topology.consumers().forEach(member -> requested.addAll(member.tables()));
        return requested;
    }

    private void startPreparedTail(Owner owner, SrsChainState opened, ConfiguredCaptureRun opening,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure, boolean recoverMismatch) {
        owner.require();if(owner.physical!=null || owner.configured==null || !opened.miningChainId().equals(owner.prepared.chainId()))throw refused();
        var config=new CaptureConfig(owner.config.connectorId(),owner.config.settings(),opened.servingTables().stream().sorted().toList(),owner.config.node(),owner.config.sharedNotes(),owner.config.nodeConfig());
        var start=opened.resumeToken()==null?CaptureStart.present():CaptureStart.resume(new SourcePosition(opened.resumeToken()));
        var physical=new Physical(owner);owner.physical=physical;
        var captured = new AtomicReference<io.tapstate.core.common.FailureFact>();
        boolean lockedStartup = Thread.holdsLock(this);
        java.util.function.Function<io.tapstate.runtime.srs.SrsCaptureWriter, CapturePort> open = writer -> {
            CapturePort port=ports.open(opening,config,physical.guard,null,actual -> writer.prepare(actual,opening.preparedModelComparison(config,stores.notes(),connectors,physical.guard)));
            if(port instanceof io.tapstate.adapters.pdk.PdkCapturePort pdk){
                pdk.withOpeningFailure(captured::set).withReadRetries(retryPolicies)
                        .withStreamRecovery(failure -> {
                            if (owner.physical == physical && physical.live.get()) owner.retry.interrupted(failure);
                        }, () -> {
                            if (owner.physical == physical && physical.live.get()) owner.retry.recovered();
                        }, owner.retry::inProgress);
                if (lockedStartup) pdk.withDeferredPreflightClose(SrsBaselineModelMismatch.class::isInstance);
                if (owner.identity != null) {
                    physical.reports=operations.open(owner.identity,owner.prepared.captureId()+":stream",physical::requireReport,physical::requireReportLocal);
                    pdk.withOperationObserver(physical.reports);
                }
            }
            return port;
        };
        try {
            physical.tail = opening.startupArtifact() == null
                    ? runtime.startTail(stores.scope(), opened, owner.fence, config, start, open, physical.guard, owner.positionEditsRestricted)
                    : runtime.startTail(stores.scope(), opened, owner.fence, config, start, open, physical.guard, owner.positionEditsRestricted, true);
        } catch(RuntimeException|Error failure) {
            var fact = captured.get();
            if (fact != null && fact.cause() == failure) openingFailure.accept(fact);
            // Only the first preflight may resume. Its caller will wait after the native opening has unwound.
            if (recoverMismatch && failure instanceof SrsBaselineModelMismatch) throw failure;
            Runnable release = () -> closeFailedTail(owner, physical, failure);
            if (lockedStartup) scheduleOwnerOperation(owner, release);
            else release.run();
            throw failure;
        } finally {
            // A shared physical tail must not retain a pipeline's opening receiver.
            captured.set(null);
        }
    }

    private void closeFailedTail(Owner owner, Physical physical, Throwable failure) {
        try { physical.close(); owner.physical = null; }
        catch (RuntimeException | Error cleanup) {
            if (cleanup != failure) failure.addSuppressed(cleanup);
        }
    }

    private synchronized void awaitMismatchClose(Owner owner, Runnable release, Throwable mismatch) {
        CompletableFuture<Void> reservation = reserveOwnerOperation(owner);
        var closedTail = new CompletableFuture<Void>();
        ownerOperations.execute(() -> {
            try { release.run(); }
            finally {
                synchronized (this) {
                    closedTail.complete(null);
                    notifyAll();
                }
            }
        });
        // Keep the original owner's reservation while the coordinator monitor is released.
        try {
            while (!closedTail.isDone()) wait();
            finishOwnerOperation(owner, reservation);
        } catch (InterruptedException interrupted) {
            closedTail.whenComplete((ignored, failure) -> finishOwnerOperation(owner, reservation));
            mismatch.addSuppressed(StorageRuntime.unavailable());
            Thread.currentThread().interrupt();
        }
    }
    private RingNotOpenYet awaiting(Source source) {
        var key=source.consumer.context().key();long now=System.nanoTime();long began=waiting.computeIfAbsent(key,ignored -> now);
        var ttl=execution.ownership().ttl();if(now-began>=ttl.toNanos())throw new TapstateException(CaptureError.NO_RING_TO_ATTACH,
                Map.of("captureId",source.prepared.captureId(),"seconds",ttl.toSeconds()),null);
        return new RingNotOpenYet(new CaptureId(source.prepared.captureId()));
    }
    private record DemandCheck(Owner owner, long revision) {}

    private void maintenanceTick() {
        maintain();
        List<DemandCheck> due = new ArrayList<>();
        synchronized (this) {
            if (closed.get()) return;
            long now = System.nanoTime();
            for (Owner owner : owners.values()) {
                if (!owner.ended.get() && now - owner.nextDemandCheck >= 0) {
                    owner.nextDemandCheck = now + execution.renewal().toNanos();
                    due.add(new DemandCheck(owner, owner.demandRevision));
                }
            }
        }
        for (DemandCheck check : due) {
            Owner owner = check.owner();
            boolean required = true;
            try {
                required = needed(owner);
            } catch (RuntimeException unavailable) {
                LOG.debug("Could not check demand for capture {}", owner.prepared.captureId(), unavailable);
            }
            synchronized (this) {
                if (owners.get(owner.prepared.captureId()) == owner && !owner.ended.get()
                        && owner.demandRevision == check.revision()) owner.demand.set(required);
            }
        }
    }

    private record ForeignCheck(Session session, Source source, WorkloadClaimFence fence, boolean live, Throwable failure) {}

    private Map<Source, ForeignCheck> checkForeignCaptures() {
        var selected = new ArrayList<ForeignCheck>();
        synchronized (this) {
            if (closed.get()) return Map.of();
            for (var session : sessions.values()) if (!session.starting && session.failure.get() == null && !session.ended.get())
                for (var source : session.sources.values()) if (source.shared
                        && (source.owner == null || source.owner.ended.get() && !source.owner.handingOff))
                    selected.add(new ForeignCheck(session, source, source.foreign, false, null));
        }
        var checked = new HashMap<Source, ForeignCheck>();
        for (var original : selected) {
            boolean live = false;
            Throwable failure = null;
            try {
                requireSession(original.session());
                if (original.fence() != null) {
                    try { execution.authorization().requireCapture(stores.scope(), original.fence()); live = true; }
                    catch (TapstateException expired) { /* Re-read the same prepared capture before takeover. */ }
                }
            } catch (RuntimeException | Error failed) { failure = failed; }
            checked.put(original.source(), new ForeignCheck(original.session(), original.source(), original.fence(), live, failure));
        }
        return checked;
    }

    private void maintain() {
        applyMaintenance(checkForeignCaptures());
    }

    private synchronized void applyMaintenance(Map<Source, ForeignCheck> foreign) {
        if(closed.get())return;
        for(var session:List.copyOf(sessions.values()))if(!session.starting && session.failure.get()==null && !session.ended.get())for(var source:session.sources.values())if(source.shared && (source.owner==null || source.owner.ended.get() && !source.owner.handingOff)) {
            var openingFailure = new AtomicReference<io.tapstate.core.common.FailureFact>();
            try {
                var check = foreign.get(source);
                if (check == null || check.session() != session || sessions.get(session.original) != session
                        || !Objects.equals(check.fence(), source.foreign)) continue;
                if (check.failure() != null) throwFailure(check.failure());
                if (check.live()) continue;
                requireSessionLocal(session);
                var prepared=stores.preparations().load(source.prepared);source.owner=owner(session,source,prepared);
                if (source.owner != null) {
                    source.owner.positionEditsRestricted |= source.positionEditsRestricted;
                    bindOpener(source.owner, session, source);
                }
            } catch (RingNotOpenYet pending) {
                // The original owner is still opening or closing its physical tail.
            } catch(RuntimeException|Error failure){
                var captured = openingFailure.get();
                session.failure.compareAndSet(null, captured != null && captured.cause() == failure
                        ? captured : new io.tapstate.core.common.FailureFact(failure));
            }
        }
        for (var owner : List.copyOf(owners.values())) {
            if (owner.prepared.mode() == CapturePreparation.Mode.SHARED_SRS
                    && (!owner.ended.get() || owner.handingOff || owner.maintenanceRetirementPending))
                scheduleOwnerOperation(owner, () -> maintainOwner(owner));
        }
    }

    private void scheduleOwnerOperation(Owner owner, Runnable operation) {
        if (ownerOperations.isShutdown() || owner.stopping) return;
        var attempt = reserveOwnerOperation(owner);
        ownerOperations.execute(() -> {
            try { operation.run(); }
            finally { finishOwnerOperation(owner, attempt); }
        });
    }

    private CompletableFuture<Void> reserveOwnerOperation(Owner owner) {
        owner.stopping = true;
        var attempt = new CompletableFuture<Void>();
        owner.operationAttempt = attempt;
        return attempt;
    }

    private void finishOwnerOperation(Owner owner, CompletableFuture<Void> attempt) {
        synchronized (this) {
            owner.stopping = false;
            owner.operationAttempt = null;
        }
        attempt.complete(null);
    }

    private void maintainOwner(Owner owner) {
        if (owner.ended.get()) {
            try {
                if (owner.handingOff) finishLost(owner);
                else if (owner.maintenanceRetirementPending) retireMaintainedOwner(owner);
            }
            catch (RuntimeException unavailable) {
                String reason = unavailable instanceof TapstateException coded
                        ? coded.code().code() : unavailable.getClass().getSimpleName();
                LOG.warn("Capture cleanup deferred for {} [{}]", owner.prepared.captureId(), reason);
            }
            return;
        }
        var openingFailure = new AtomicReference<io.tapstate.core.common.FailureFact>();
        try {
            owner.require();
            if (owner.drainingReports()) {
                if (!needed(owner)) retireMaintainedOwner(owner);
                return;
            }
            var physical = owner.physical;
            var failure = physical == null ? Optional.<io.tapstate.core.common.FailureFact>empty() : physical.failureFact();
            if (failure.isEmpty()) {
                if (owner.retry.waiting()) recoverSharedRead(owner, physical, openingFailure);
                else maintainTail(owner, openingFailure::set);
                return;
            }
            var fact = failure.orElseThrow();
            if (duringHandoff(owner, fact.cause())) return;
            markOwnerFailure(owner, fact);
            owner.reportDrainStarted = reportClock.getAsLong();
            // Stop I/O while the original ownership proof still covers captured reports.
            physical.closeIo();
            if (!needed(owner)) retireMaintainedOwner(owner);
        } catch (RuntimeException | Error failure) {
            if (duringHandoff(owner, failure)) return;
            var captured = openingFailure.get();
            markOwnerFailure(owner, captured != null && captured.cause() == failure ? captured
                    : new io.tapstate.core.common.FailureFact(failure));
            try { retireMaintainedOwner(owner); }
            catch (RuntimeException | Error ending) {
                if (ending != failure) failure.addSuppressed(ending);
                LOG.warn("Could not retire capture {} after maintenance", owner.prepared.captureId(), failure);
            }
        }
    }

    private void recoverSharedRead(Owner owner, Physical physical, AtomicReference<FailureFact> openingFailure) {
        if (physical != null) {
            owner.finishCheckpointingForRetirement();
            physical.close();
            owner.physical = null;
        }
        if (!owner.demand.get() && !needed(owner)) {
            retireMaintainedOwner(owner);
            return;
        }
        var pending = owner.retry.decision(streamRetryPolicy());
        if (pending == null) return;
        if (pending.delay() == null) {
            markOwnerFailure(owner, pending.failure());
            retireMaintainedOwner(owner);
            return;
        }
        if (!pending.delay().isZero()) return;
        owner.retry.attempting();
        LOG.info("Reopening shared capture {} from its durable checkpoint", owner.prepared.captureId());
        try {
            maintainTail(owner, openingFailure::set);
        } catch (RuntimeException opening) {
            if (!owner.retry.waiting() || owner.physical != null) throw opening;
        }
    }

    private void markOwnerFailure(Owner owner, io.tapstate.core.common.FailureFact failure) {
        synchronized (this) {
            for (var session : sessions.values()) {
                if (session.sources.values().stream().anyMatch(source -> source.owner == owner))
                    session.failure.compareAndSet(null, failure);
            }
        }
    }

    private void retireMaintainedOwner(Owner owner) {
        synchronized (this) {
            owner.maintenanceRetirementPending = true;
            owner.ended.set(true);
        }
        owner.release();
        synchronized (this) { owners.remove(owner.prepared.captureId(), owner); }
    }

    private void maintainTail(Owner owner,
            java.util.function.Consumer<io.tapstate.core.common.FailureFact> openingFailure) {
        var current = tailStores(owner);
        String chainId = owner.prepared.chainId();
        SrsConsumerStore.RetentionSnapshot snapshot = null;
        try {
            snapshot = current.consumers().retentionSnapshot(chainId).orElse(null);
        } catch (TapstateException unavailable) {
            if (unavailable.code() != IoError.STORE_UNAVAILABLE) throw unavailable;
            owner.require();
        }
        var original = owner.physical;
        ensureTail(owner, current, false, snapshot, openingFailure, false);
        try {
            // An epoch change replaces the physical tail. Read retention floors again for the new epoch.
            if (snapshot != null && owner.physical == original)
                runtime.retain(stores.scope(), chainId, owner.fence, owner::require, snapshot);
            else
                runtime.retain(stores.scope(), chainId, owner.fence, owner::require);
        } catch (TapstateException unavailable) {
            if (unavailable.code()!=IoError.STORE_UNAVAILABLE) throw unavailable;
            // Cleanup can retry from current confirmed floors; capture and ownership failures remain fatal.
            owner.require();
            LOG.warn("Retention for capture {} was deferred [{}]",owner.prepared.captureId(),unavailable.code().code());
        }
    }
    private static void throwFailure(Throwable failure){if(failure instanceof RuntimeException runtime)throw runtime;if(failure instanceof Error error)throw error;throw new IllegalStateException("capture failed",failure);}
    private static boolean duringHandoff(Owner owner, Throwable failure) {
        return owner.handingOff && failure instanceof TapstateException coded
                && coded.code() == IoError.PIPELINE_INTENT_FENCED;
    }

    private void lost(Owner owner) {
        owner.handingOff = owner.prepared.mode() == CapturePreparation.Mode.SHARED_SRS;
        owner.ended.set(true);
        CompletableFuture<Void> attempt;
        synchronized (this) {
            if (hostClosing || owners.get(owner.prepared.captureId()) != owner || owner.stopping) return;
            attempt = reserveOwnerOperation(owner);
        }
        try { finishLost(owner); }
        finally { finishOwnerOperation(owner, attempt); }
    }

    private void finishLost(Owner owner) {
        List<CaptureRun> privateRuns = new ArrayList<>();
        synchronized (this) {
            if (owners.get(owner.prepared.captureId()) != owner) return;
            for (var session : sessions.values()) for (var source : session.sources.values()) {
                if (source.owner == owner && !source.shared) {
                    session.failure.compareAndSet(null, new io.tapstate.core.common.FailureFact(refused()));
                    if (source.run != null) privateRuns.add(source.run);
                }
            }
        }
        Throwable failure = null;
        if (owner.physical != null) {
            owner.physical.close();
            owner.physical = null;
        }
        for (CaptureRun run : privateRuns) failure = cleanup(failure, () -> runtime.stopSnapshot(run));
        rethrow(failure);
        // An exited owner's release remains retryable even when its renewer closed on loss.
        owner.release();
        synchronized (this) {
            for (var session : sessions.values()) for (var source : session.sources.values()) {
                if (source.owner == owner && source.shared) {
                    source.owner = null;
                    source.foreign = owner.fence;
                }
            }
            owners.remove(owner.prepared.captureId(), owner);
        }
    }
    private void stop(Session session) {
        stop(session, null);
    }

    private void stop(Session session, Long shutdownDeadline) {
        synchronized (this) {
            if (sessions.get(session.original) != session) return;
            if (session.starting || session.stopping) throw refused();
            session.stopping = true;
            session.ended.set(true);
        }
        try {
            stopSessionIo(session);
            Set<Owner> originalOwners;
            synchronized (this) {
                originalOwners = Collections.newSetFromMap(new IdentityHashMap<Owner,Boolean>());
                session.sources.values().forEach(source -> { if (source.owner != null) originalOwners.add(source.owner); });
            }
            for (var owner : originalOwners) detachAndRetire(session, owner, shutdownDeadline);
            synchronized (this) { forgetSession(session); }
        } finally {
            synchronized (this) { session.stopping = false; }
        }
    }

    private void stopSessionIo(Session session) {
        Throwable first = null;
        stores.boundedLoads().forget(session.original);
        for (var source : session.sources.values()) {
            if (source.run != null) first = cleanup(first, () -> runtime.stopSnapshot(source.run));
            if (source.guard != null) first = cleanup(first, () -> runtime.stopOpenings(source.guard));
            if (session.bindings != null)
                first = cleanup(first, () -> buffer.release(PipelineCaptureBindings.bufferKey(session.original,source.id)));
        }
        rethrow(first);
    }

    private void detachAndRetire(Session session, Owner owner, Long shutdownDeadline) {
        CompletableFuture<Void> retirement;
        for (;;) {
            CompletableFuture<Void> preceding;
            synchronized (this) {
                if (owners.get(owner.prepared.captureId()) != owner) return;
                if (owner.sessions.remove(session.original)) owner.demandRevision++;
                if (!owner.sessions.isEmpty()) return;
                preceding = owner.operationAttempt;
                if (preceding == null) {
                    retirement = reserveOwnerOperation(owner);
                    break;
                }
            }
            if (shutdownDeadline == null) waitForOwner(preceding);
            else ShutdownAwait.future(preceding, shutdownDeadline);
        }
        try {
            try {
                if (!owner.ended.get() && needed(owner)) return;
            } catch (TapstateException fenced) {
                // Handoff may end this exact shared owner while Stop holds its retirement reservation.
                if (!owner.ended.get() || !duringHandoff(owner, fenced)) throw fenced;
            }
            owner.finishCheckpointingForRetirement();
            owner.ended.set(true);
            owner.release();
            synchronized (this) { owners.remove(owner.prepared.captureId(), owner); }
        } finally { finishOwnerOperation(owner, retirement); }
    }

    private static void waitForOwner(CompletableFuture<Void> attempt) {
        try { attempt.get(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        } catch (ExecutionException failed) { throwFailure(failed.getCause()); }
    }

    private static Throwable cleanup(Throwable first,Runnable cleanup) {
        try { cleanup.run(); }
        catch(RuntimeException|Error failure) {
            if(first==null)return failure;
            if(first!=failure)first.addSuppressed(failure);
        }
        return first;
    }
    private static void rethrow(Throwable failure) {
        if(failure instanceof RuntimeException runtime)throw runtime;
        if(failure instanceof Error error)throw error;
    }
    private static StartFrom readStart(PipelineResource pipeline) {
        var settings=pipeline.settings();return StartFrom.parse(settings==null || settings.startFrom()==null?"latest":settings.startFrom());
    }
    private static CaptureStart sourceStart(PipelineResource pipeline) {
        var start=readStart(pipeline);
        return switch(start) {
            case StartFrom.Earliest ignored -> CaptureStart.earliest();
            case StartFrom.Latest ignored -> CaptureStart.present();
            case StartFrom.At at -> CaptureStart.at(at.instant());
        };
    }
    private void requireSessionLocal(Session session) {active();if(session.ended.get())throw refused();execution.authorization().requireLocal(session.original);}
    private void requireSession(Session session) { active(); if(session.ended.get())throw refused(); requirePipeline(session.original); }
    private void requirePipeline(ExecutionFence original) { active(); if(!claim(original).equals(execution.authorization().require(original)))throw refused(); }
    private void active() { execution.active().run(); if(closed.get())throw refused(); }
    private static WorkloadClaimFence claim(ExecutionFence original) {
        return new WorkloadClaimFence(new WorkloadClaimKey(original.scope().clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,original.pipelineId()),
                new WorkloadOwner(original.ownerNodeId(),original.ownerBootId()),original.claimGeneration(),original.executionGeneration(),original.topologyRevision());
    }
    private static CacheBusinessScope business(PipelineIntentScope scope) { return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()); }
    private static TapstateException refused() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null); }
    @Override public void close() {
        synchronized (hostShutdown) {
            List<CompletableFuture<Void>> active;
            synchronized (this) {
                hostClosing = true;
                maintenance.shutdownNow();
                ownerOperations.shutdown();
                active = owners.values().stream().map(owner -> owner.operationAttempt).filter(Objects::nonNull).toList();
            }
            long deadline = ShutdownAwait.deadline();
            ShutdownAwait.executor(ownerOperations, deadline);
            for (var attempt : active) ShutdownAwait.future(attempt, deadline);
            closeRetained(deadline);
        }
    }

    private void closeRetained(long deadline) {
        List<Owner> capturedOwners;
        List<Session> capturedSessions;
        synchronized (this) {
            capturedOwners = List.copyOf(owners.values());
            capturedSessions = List.copyOf(sessions.values());
        }
        Throwable first = null;
        if (!closed.get()) {
            for (var owner : capturedOwners)
                first = cleanup(first, () -> withShutdownOwner(owner, deadline, owner::finishCheckpointingForRetirement));
        }
        closed.set(true);
        capturedOwners.forEach(owner -> owner.ended.set(true));
        for (var session : capturedSessions) first = cleanup(first, () -> stop(session, deadline));
        for (var owner : capturedOwners) {
            first = cleanup(first, () -> withShutdownOwner(owner, deadline, () -> {
                synchronized (this) {
                    if (sessions.values().stream().anyMatch(session -> session.sources.values().stream()
                            .anyMatch(source -> source.owner == owner))) return;
                }
                retireMaintainedOwner(owner);
            }));
        }
        rethrow(first);
    }

    private void withShutdownOwner(Owner owner, long deadline, Runnable operation) {
        CompletableFuture<Void> reservation;
        for (;;) {
            CompletableFuture<Void> preceding;
            synchronized (this) {
                if (owners.get(owner.prepared.captureId()) != owner) return;
                preceding = owner.operationAttempt;
                if (preceding == null) {
                    reservation = reserveOwnerOperation(owner);
                    break;
                }
            }
            ShutdownAwait.future(preceding, deadline);
        }
        try { operation.run(); }
        finally { finishOwnerOperation(owner, reservation); }
    }
}
