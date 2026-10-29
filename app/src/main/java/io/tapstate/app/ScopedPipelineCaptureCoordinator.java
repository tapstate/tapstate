package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.runtime.srs.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

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
    private final Map<ExecutionFence,Session> sessions=new LinkedHashMap<>();
    private final Map<String,Owner> owners=new LinkedHashMap<>();
    private final AtomicReference<Predicate<String>> driving=new AtomicReference<>(id -> true);
    private final AtomicBoolean closed=new AtomicBoolean();
    private final Map<SrsConsumerKey,Long> waiting=new HashMap<>();
    private final ScheduledExecutorService maintenance=Executors.newSingleThreadScheduledExecutor(task -> {
        var thread=new Thread(task,"tapstate-native-shared-capture");thread.setDaemon(true);return thread;
    });

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
        this.stores=Objects.requireNonNull(stores); this.execution=Objects.requireNonNull(execution);
        this.runtime=Objects.requireNonNull(runtime); this.buffer=Objects.requireNonNull(buffer);
        this.connectors=Objects.requireNonNull(connectors); this.ports=Objects.requireNonNull(ports);
        maintenance.scheduleWithFixedDelay(this::maintain,1,1,TimeUnit.SECONDS);
    }
    @Override public void runOnlyWhileDriving(Predicate<String> driving) { this.driving.set(Objects.requireNonNull(driving)); }
    @Override public PipelineCaptureCoordinator forRun(PipelineIntentScope commandScope,WorkloadClaimFence command,ExecutionFence original) {
        active(); Objects.requireNonNull(commandScope); Objects.requireNonNull(command); Objects.requireNonNull(original);
        if (original.scope()==null || !business(commandScope).equals(stores.scope()) || !business(original.scope()).equals(stores.scope())
                || !commandScope.pipelineId().equals(original.pipelineId()) || !command.key().clusterId().equals(commandScope.clusterId())
                || command.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION || !command.key().resourceId().equals(commandScope.pipelineId())) throw refused();
        return new Run(commandScope,command,original);
    }
    @Override public void startCapture(String pipeline) { throw refused(); }
    @Override public void stopCapture(String pipeline,boolean purge) { throw refused(); }

    synchronized void retireScope(PipelineIntentScope scope, WorkloadClaimFence ceiling) {
        active(); var captured = sessions.values().stream().filter(session -> session.original.scope().equals(scope)).toList();
        if (ceiling == null && !captured.isEmpty()) throw refused();
        for (var session : captured) requireNotLater(session.original, ceiling);
        for (var session : captured) { stop(session); sessions.remove(session.original, session); }
    }
    synchronized boolean hasScope(PipelineIntentScope scope) {
        active(); return sessions.keySet().stream().anyMatch(original -> original.scope().equals(scope));
    }
    /** Releases only the original local shared owner after all of its Pipeline sessions have ended. */
    synchronized void stopSharedLocally(CapturePreparation original,WorkloadClaimFence fence) {
        requireShared(original,fence);
        // This stages parallel local shutdown; the separate all-member verification still proves the durable end.
        if(owners.containsKey(original.captureId()))retireShared(original,fence);
    }
    synchronized void retireShared(CapturePreparation original,WorkloadClaimFence fence) {
        requireShared(original,fence);
        var owner=owners.get(original.captureId());
        if(owner==null){execution.ownership().requireEnded(fence);return;}
        if(!original.equals(owner.prepared) || !fence.equals(owner.fence) || !owner.sessions.isEmpty()
                || !owner.ended.get() && needed(owner))throw refused();
        owner.ended.set(true);owner.release();execution.ownership().endTenure(fence);owners.remove(original.captureId(),owner);
    }
    synchronized void requireSharedRetired(CapturePreparation original,WorkloadClaimFence fence) {
        requireShared(original,fence);if(owners.containsKey(original.captureId()))throw refused();
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
        Run(PipelineIntentScope commandScope,WorkloadClaimFence command,ExecutionFence original) {
            this.commandScope=commandScope; this.command=command; this.original=original;
        }
        @Override public void startCapture(String pipeline) { throw refused(); }
        @Override public void startCapture(String pipeline,ArtifactStore captured,SourceModelSnapshot models) {
            requireId(pipeline);
            if (!original.scope().equals(commandScope) || !claim(original).equals(command)) throw refused();
            start(this,captured,models);
        }
        @Override public void stopCapture(String pipeline,boolean purge) {
            requireId(pipeline);
            if (purge) throw refused();
            synchronized (ScopedPipelineCaptureCoordinator.this) { var session=sessions.get(original); if(session!=null) {stop(session);sessions.remove(original,session);} }
        }
        @Override public PipelineCaptureBindings captureBindings() { return session().bindings; }
        @Override public boolean isCapturing(String pipeline) { requireId(pipeline); synchronized(ScopedPipelineCaptureCoordinator.this) { return sessions.containsKey(original); } }
        @Override public Optional<Throwable> captureFailure(String pipeline) {
            requireId(pipeline); synchronized(ScopedPipelineCaptureCoordinator.this) {
                var held=sessions.get(original); if(held==null)return Optional.empty();
                if(held.failure.get()!=null)return Optional.of(held.failure.get());
                for(var source:held.sources.values())if(source.owner!=null && source.owner.physical!=null) {
                    var failure=source.owner.physical.tail.health().failure();if(failure.isPresent())return failure;
                }
                return held.sources.values().stream().filter(source -> source.run!=null).map(source -> source.run.failure()).flatMap(Optional::stream).findFirst();
            }
        }
        @Override public boolean loadDelivered(String pipeline) { requireId(pipeline); var held=current(); if(held==null || !held.snapshot || held.sources.isEmpty())return true;
            requireSession(held);if(held.bounded)return stores.boundedLoads().complete(original);
            return held.sources.values().stream().allMatch(source ->
                    streamingCursor(held,source).snapshotCompletedTables().containsAll(source.selected.tables())); }
        @Override public SnapshotReading snapshotProgress(String pipeline) { requireId(pipeline); var held=current(); return held==null?SnapshotReading.NONE:snapshotReading(held); }
        @Override public SnapshotReading runSnapshotProgress(String pipeline) { requireId(pipeline); var held=current(); return held==null?SnapshotReading.NONE:runSnapshotReading(held); }
        @Override public InitialLoad initialLoad(String pipeline, ExecutionPlan plan, Instant at) {
            requireId(pipeline); var held=current(); return held==null?null:initialLoadReading(held,plan,at);
        }
        private Session current() { synchronized(ScopedPipelineCaptureCoordinator.this) { return sessions.get(original); } }
        private Session session() { var session=current(); if(session==null || session.bindings==null)throw refused(); return session; }
        private void requireId(String pipeline) { if(!original.pipelineId().equals(pipeline))throw refused(); }
    }
    private synchronized void start(Run run,ArtifactStore artifacts,SourceModelSnapshot models) {
        active(); requirePipeline(run.original); Objects.requireNonNull(artifacts); Objects.requireNonNull(models).requirePipeline(run.original.pipelineId());
        var authored=StoredArtifacts.requirePipeline(artifacts,run.original.pipelineId());
        var pipeline=PipelineInlining.inline(authored,artifacts);
        var sourceDefinitions=new LinkedHashMap<String,SourceResource>();
        for(String id:pipeline.sourceIds()) sourceDefinitions.put(id,StoredArtifacts.requireSource(artifacts,id));
        var inputs=models.preparationInputs(run.original.scope(),authored,pipeline,sourceDefinitions);
        var existing=sessions.get(run.original);
        if(existing!=null) { if(!existing.inputs.equals(inputs))throw refused(); return; }
        var mode=pipeline.settings()==null || pipeline.settings().readMode()==null?ReadMode.SNAPSHOT_AND_CDC:pipeline.settings().readMode();
        var session=new Session(run.original,inputs,execution.clock().instant(),mode); sessions.put(run.original,session);
        try {
            var node=execution.node().get().orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if(!node.owner().equals(claim(run.original).owner()))throw refused();
            for(var definition:sourceDefinitions.values()) {
                var frozenModel=models.model(definition);
                var selected=SourceCaptureResolution.forPipeline(pipeline,definition,frozenModel);
                if(selected.isEmpty())continue;
                boolean shared=!session.bounded && pipeline.sources().stream().anyMatch(ref -> ref.id().equals(definition.id()) && ref instanceof SourceRef.Spec spec && spec.srs());
                var request=new CapturePreparationRequest(run.original.scope(),claim(run.original),node,definition.id(),inputs);
                var issued=stores.configuration().issueFirstCreate(request);
                var prepared=issued instanceof CaptureCreationResult.Issued first?stores.preparations().createFirst(first.creation()):
                        stores.preparations().load(((CaptureCreationResult.AlreadyIssued)issued).preparation());
                if(prepared.preparation().mode()!=(session.bounded?CapturePreparation.Mode.SNAPSHOT_ONLY:shared?CapturePreparation.Mode.SHARED_SRS:CapturePreparation.Mode.DIRECT))throw refused();
                var resolution=shared?selected.orElseThrow().onPhysicalChain(prepared.preparation().canonicalChainId(),prepared.preparation().chainId()):selected.orElseThrow();
                var source=new Source(definition.id(),resolution,shared,frozenModel);session.sources.put(source.id,source);
                source.prepared=prepared.preparation();source.owner=owner(session,source,prepared);
                if(shared) {
                    var history=stores.configuration().inspectSharedHistory(source.prepared,source.id,inputs.sourceHashes().get(source.id));
                    if(!source.prepared.equals(history.preparation()))throw refused();
                    source.positionEditsRestricted=history.positionEditsRestricted();
                    if(source.owner!=null)source.owner.positionEditsRestricted|=source.positionEditsRestricted;
                    source.srs=stores.sources().srs(stores.scope(),() -> requireSession(session));
                    var key=new SrsConsumerKey(source.prepared.chainId(),session.original.pipelineId(),source.id);
                    var context=new SrsConsumerContext(key,session.original.scope().stateGeneration());var held=source.srs.consumers().find(key);
                    var relation=held.filter(member -> !member.detached() && member.context().equals(context)
                            && member.tables().equals(Set.copyOf(source.selected.tables())) && member.commandFence().equals(claim(session.original)))
                            .orElseGet(() -> source.srs.consumers().attach(context,held.map(SrsConsumerMembership::revision).orElse(0L),Set.copyOf(source.selected.tables()),claim(session.original)).orElseThrow(ScopedPipelineCaptureCoordinator::refused));
                    source.consumer=SrsConsumerRun.of(session.original.scope(),relation);
                }
            }
            var snapshots=new LinkedHashMap<String,PipelineCaptureBindings.SnapshotOnlySource>();
            var state=session.snapshot?stores.pipelines().snapshotStates(run.original.scope(),claim(run.original),() -> requireSession(session)):null;
            long prior=state==null?0:state.order(); long epoch=0;
            if(session.bounded && !session.sources.isEmpty()) {
                var captures=new LinkedHashMap<String,CaptureCacheState>();
                session.sources.forEach((id,source) -> captures.put(id,new CaptureCacheState(source.owner.prepared,source.owner.fence,Optional.empty())));
                epoch=Objects.requireNonNull(state,"bounded capture requires snapshot state").advanceOrder(prior,prior,captures);
                for(var source:session.sources.values())snapshots.put(source.id,new PipelineCaptureBindings.SnapshotOnlySource(source.selected.chainId().value(),Set.copyOf(source.selected.tables()),epoch));
            }
            var direct=new LinkedHashMap<String,DirectSourceRun>();
            if(!session.bounded)for(var source:session.sources.values()) {
                if(source.shared)continue;
                Runnable guard=() -> {requireSession(session);source.owner.require();};
                var preparation=inputs.directChannels().stream().filter(value -> value.context().sourceId().equals(source.id)).findFirst().orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                if(!preparation.context().captureId().equals(source.owner.prepared.captureId()))throw refused();
                source.channels=stores.sources().directChannels(stores.scope(),guard);
                var before=source.channels.load(preparation.context());
                long allocated=source.channels.beginCapture(preparation.context(),before.revision(),before.epoch(),source.owner.fence,claim(run.original))
                        .orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                source.direct=source.channels.load(preparation.context());
                if(source.direct.epoch()!=allocated)throw refused();
                var binding=DirectSourceRun.of(run.original,source.direct);direct.put(source.id,binding);
                var cursor=source.direct.cursor();
                String token=source.direct.resumeToken();
                source.start=token==null?sourceStart(pipeline):CaptureStart.resume(new SourcePosition(token));
            }
            var logged=new LinkedHashMap<String,PipelineCaptureBindings.LoggedSource>();
            for(var source:session.sources.values())if(source.shared) {
                var before=source.srs.chains().load(source.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                var retained=source.srs.consumers().progress(source.consumer.context()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                if(!session.snapshot && readStart(pipeline) instanceof StartFrom.At at
                        && at.instant().isBefore(execution.clock().instant())
                        && !retained.ringDoneThrough().keySet().containsAll(source.selected.tables())
                        && (before.epoch()==0 || !before.servingTables().containsAll(source.selected.tables()))) {
                    throw new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW,Map.of(
                            "requested",at.instant().toString(),"earliest",execution.clock().instant().toString(),
                            "retention",before.retention()==null?"unset":before.retention()),null);
                }
                if(source.owner!=null) {
                    if(source.owner.config==null){source.owner.origin=session.original;source.owner.config=source.selected.config().at(new PipelineNode(session.original.pipelineId(),source.id))
                            .sharing(new SharedNotes(source.prepared.canonicalChainId(),List.of()));}
                    try { ensureTail(source.owner); }
                    catch(SrsBaselineModelMismatch mismatch) {
                        if(!session.snapshot || retained.snapshot()!=null || before.pendingFreshFull()!=null
                                || !source.prepared.chainId().equals(mismatch.chainId())
                                || source.owner.physical!=null || mismatch.getSuppressed().length!=0)throw mismatch;
                        var full=stores.completedFull().find(run.original.scope(),inputs,PipelineInputClosure.dependencyHashes(artifacts,authored,inputs))
                                .orElseThrow(() -> mismatch);
                        source.freshFull=source.srs.consumers().inspectFreshFull(full,source.consumer,retained.revision(),node,source.owner.fence);
                    }
                }
                var chain=source.srs.chains().load(source.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                if(source.freshFull!=null) { waiting.remove(source.consumer.context().key());continue; }
                if(chain.epoch()==0 || !chain.servingTables().containsAll(source.selected.tables()))throw awaiting(source);
                var rings=new TreeMap<String,String>();source.selected.tables().forEach(table -> rings.put(table,source.selected.ringName(table)));
                var published=source.srs.models().publishedForNewSnapshot(source.prepared.chainId(),rings);
                if(published.isEmpty() || published.orElseThrow().values().stream().anyMatch(model -> model.key().captureEpoch()!=chain.epoch()))throw awaiting(source);
                // Metadata preflight precedes the asynchronous source-start baseline. Keep this original live tail.
                // A later owner still requires the existing checkpoint before it can open another capture epoch.
                var progress=source.srs.consumers().progress(source.consumer.context()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                var arrivals = new LinkedHashMap<String,Long>();
                for(String table:source.selected.tables())if(!progress.ringDoneThrough().containsKey(table)) {
                    var start=session.snapshot?StartFrom.latest():readStart(pipeline);
                    long after;
                    if(chain.pendingFreshFull()!=null) {
                        var key=chain.pendingFreshFull().models().get(table);if(key==null)throw refused();
                        var fixed=source.srs.models().require(key);if(!fixed.ringName().equals(source.selected.ringName(table)))throw refused();
                        after=fixed.ddlSequence()-1;
                    } else after=runtime.initialReadAfter(stores.scope(),source.prepared.chainId(),table,start,chain.retention(),() -> requireSession(session));
                    arrivals.put(table,after);
                }
                if (!arrivals.isEmpty()) {
                    if(!source.srs.consumers().startReadingAfter(source.consumer.context(),source.consumer.membershipRevision(),progress.revision(),arrivals,claim(session.original)))throw refused();
                    progress=source.srs.consumers().progress(source.consumer.context()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
                }
                waiting.remove(source.consumer.context().key());
                long originalEpoch=progress.snapshot()!=null?progress.snapshot().snapshotEpoch():chain.pendingFreshFull()!=null?chain.pendingFreshFull().snapshotEpoch():chain.epoch();
                logged.put(source.id,new PipelineCaptureBindings.LoggedSource(source.consumer,originalEpoch,progress.ringDoneThrough(),source.prepared.canonicalChainId()));
            }
            // The preparation mode belongs to the state generation; its READY receipt suppresses repeated target clearing.
            session.bindings=new PipelineCaptureBindings(run.original,logged,direct,snapshots,session.snapshot && !session.sources.isEmpty());
            for(var source:session.sources.values()) {source.snapshotEpoch=epoch;source.readTables=Set.copyOf(source.selected.tables());}
            for(var source:session.sources.values()) {
                String handoff=source.freshFull==null?session.bindings.bufferKey(source.id):PipelineCaptureBindings.bufferKey(run.original,source.id);
                var declared=new LinkedHashMap<String,CaptureHandoff>();
                for(String table:source.selected.tables()) {
                    String ring=source.selected.ringName(table); if(session.snapshot)buffer.declareSnapshot(handoff,ring);
                    declared.put(table,buffer.handoff(handoff,ring));
                }
                var originalHandoffs=Map.copyOf(declared);
                Owner originalOwner=source.owner;
                Runnable guard=source.shared?() -> requireSession(session):() -> { requireSession(session); originalOwner.require(); };source.guard=guard;
                if(source.shared && !session.snapshot)continue;
                var config=source.selected.config().at(new PipelineNode(run.original.pipelineId(),source.id));
                if(source.shared)config=config.sharing(new SharedNotes(source.prepared.canonicalChainId(),List.of()));
                var fixedConfig=config;
                var notes=ConfiguredCaptureRun.prepare(run.original,config,stores.notes(),connectors,guard,stores.recoveryPinned().test(run.original.scope()));
                CaptureHandoff output=new CaptureHandoff() {
                    @Override public void accept(Envelope row) {
                        guard.run(); if(!source.selected.tables().contains(row.src()))throw refused();
                        originalHandoffs.get(row.src()).accept(row);
                        if(row.op()==Op.READ)source.rows.merge(row.src(),1L,Long::sum);
                    }
                    @Override public void loaded(String table) {
                        guard.run(); if(!session.snapshot || !source.selected.tables().contains(table))throw refused();
                        if(source.readTables.contains(table))state.recordCount(source.id,table,source.snapshotEpoch,source.rows.getOrDefault(table,0L));
                        guard.run(); source.completed.add(table); originalHandoffs.get(table).loaded(table);
                    }
                };
                if(source.shared) {
                    var binding=logged.get(source.id);
                    long firstEpoch=source.freshFull==null?binding.snapshotEpoch():source.freshFull.originalChain().epoch();
                    Function<SrsSnapshotPreparation,CapturePort> prepareSnapshot=preparation -> {
                        source.snapshotEpoch=preparation.request().snapshotEpoch();source.readTables=Set.copyOf(preparation.stillOwed());
                        return ports.open(notes,fixedConfig,guard,(request,seam,actual) -> {
                            requireSnapshotModels(source,actual);preparation.prepare(request,seam,actual);
                        },null);
                    };
                    if(source.freshFull==null)source.run=runtime.beginSnapshot(source.consumer,fixedConfig,firstEpoch,prepareSnapshot,output,guard);
                    else source.run=runtime.beginSnapshot(source.consumer,fixedConfig,firstEpoch,prepareSnapshot,output,guard,source.freshFull,session.began,receipt -> {
                        guard.run();startPreparedTail(source.owner,receipt.chain());guard.run();
                        logged.put(source.id,new PipelineCaptureBindings.LoggedSource(source.consumer,receipt.progress().snapshot().snapshotEpoch(),receipt.progress().ringDoneThrough(),source.prepared.canonicalChainId()));
                    });
                } else if(!session.bounded && session.snapshot) {
                    source.run=runtime.beginDirectSnapshot(config,source.direct,source.channels,preparation -> {
                        source.snapshotEpoch=preparation.request().snapshotEpoch();source.readTables=Set.copyOf(preparation.stillOwed());
                        return ports.open(notes,fixedConfig,guard,preparation::prepare,null);
                    },output,buffer.capacity(),guard);
                    source.direct=source.channels.load(source.direct.context());direct.put(source.id,DirectSourceRun.of(run.original,source.direct));
                } else {
                    var port=ports.open(notes,config,guard,null,null);
                    source.run=session.bounded?runtime.beginSnapshotOnly(config,epoch,port,output,guard):
                            runtime.startDirect(config,source.direct,source.channels,source.start,port,output,buffer.capacity(),guard);
                }
                guard.run();
            }
            session.bindings=new PipelineCaptureBindings(run.original,logged,direct,snapshots,session.snapshot && !session.sources.isEmpty());
            requireSession(session);
        } catch(RuntimeException|Error failure) {
            try { stop(session);sessions.remove(run.original,session); } catch(RuntimeException|Error cleanup) { if(cleanup!=failure)failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private Owner owner(Session session,Source source,CaptureCacheState prepared) {
        var id=prepared.preparation().captureId(); var existing=owners.get(id);
        if(existing!=null) {
            if(existing.ended.get())throw refused();
            if(!existing.prepared.equals(prepared.preparation()))throw refused(); source.owner=existing; existing.require();
            if(!existing.lease.retain(() -> existing.sessions.add(session.original)))throw refused(); return existing;
        }
        var permit=execution.ownership().acquire(new CaptureId(id));
        if(!permit.acquired()) {
            if(!source.shared)throw new RingNotOpenYet(new CaptureId(id));
            source.foreign=stores.preparations().load(prepared.preparation()).claim();return null;
        }
        if(permit.fence()==null)throw refused();
        var owner=new Owner(prepared.preparation(),permit.claim()); owners.put(id,owner); owner.sessions.add(session.original); source.owner=owner;
        owner.lease=new CaptureClaimLease(execution.ownership(),permit.claim(),execution.renewal(),() -> lost(owner));
        owner.lease.whileNeeded(() -> needed(owner));
        owner.require(); return owner;
    }
    private final class Owner {
        final CapturePreparation prepared; final WorkloadClaimFence fence; final WorkloadClaim initial;
        final Set<ExecutionFence> sessions=ConcurrentHashMap.newKeySet(); final AtomicBoolean ended=new AtomicBoolean();
        CaptureClaimLease lease;ExecutionFence origin;CaptureConfig config;ConfiguredCaptureRun configured;Physical physical;boolean positionEditsRestricted;
        Owner(CapturePreparation prepared,WorkloadClaim initial) { this.prepared=prepared;this.initial=initial;this.fence=WorkloadClaimFence.from(initial); }
        void release() {
            if(physical!=null) {physical.close();physical=null;}
            if(lease!=null)lease.closeAfterPhysicalExit();else execution.ownership().release(initial);
        }
        void require() { active(); if(ended.get() || !fence.equals(execution.authorization().requireCapture(stores.scope(),fence)))throw refused(); }
    }
    private static final class Source {
        Runnable guard;
        final String id; final SourceCaptureResolution selected;final boolean shared;boolean positionEditsRestricted; final Map<String,Long> rows=new ConcurrentHashMap<>();
        final Set<String> completed=ConcurrentHashMap.newKeySet();
        final Map<String,SourceTable> expectedModels;
        Owner owner;CapturePreparation prepared;WorkloadClaimFence foreign;SrsStores srs;SrsConsumerRun consumer;SrsFreshFullPreparation freshFull; CaptureRun run; DirectChannelStore channels; DirectChannelProgress direct; CaptureStart start; long snapshotEpoch; Set<String> readTables=Set.of();
        Source(String id,SourceCaptureResolution selected,boolean shared,SourceModel frozen) {
            this.id=id;this.selected=selected;this.shared=shared;
            if(frozen==null)expectedModels=null;
            else {
                var fixed=new LinkedHashMap<String,SourceTable>();
                for(var table:frozen.tables())if(selected.tables().contains(table.name()))
                    if(fixed.put(table.name(),new SourceTable(table.name(),table.fields(),table.primaryKey(),table.indexes()))!=null)
                        throw modelUnproven(this);
                expectedModels=Map.copyOf(fixed);
            }
        }
    }
    private static void requireSnapshotModels(Source source,Map<String,SourceTable> actual) {
        if(source.expectedModels==null) {if(source.freshFull!=null)throw modelUnproven(source);return;}
        for(var entry:actual.entrySet()) {
            var table=entry.getValue();
            if(!entry.getKey().equals(table.name()) || !Objects.equals(source.expectedModels.get(entry.getKey()),
                    new SourceTable(table.name(),table.fields(),table.primaryKey(),table.indexes())))throw modelUnproven(source);
        }
    }
    private static TapstateException modelUnproven(Source source) {
        return new TapstateException(IoError.SRS_FULL_PREPARATION_UNPROVEN,Map.of("chain",source.selected.chainId().value()),null);
    }
    private static final class Session {
        final ExecutionFence original; final PipelinePreparationInputs inputs; final Instant began; final ReadMode mode; final boolean bounded,snapshot;
        final Map<String,Source> sources=new LinkedHashMap<>(); final AtomicBoolean ended=new AtomicBoolean(); final AtomicReference<Throwable> failure=new AtomicReference<>();
        PipelineCaptureBindings bindings;
        Session(ExecutionFence original,PipelinePreparationInputs inputs,Instant began,ReadMode mode) { this.original=original;this.inputs=inputs;this.began=began;this.mode=mode;this.bounded=mode==ReadMode.SNAPSHOT_ONLY;this.snapshot=mode!=ReadMode.CDC_ONLY; }
        String label(String source,String table) {
            return sources.values().stream().filter(item -> item.selected.tables().contains(table)).count()>1?source+"."+table:table;
        }
    }
    private SnapshotReading snapshotReading(Session session) {
        requireSession(session); if(!session.snapshot)return SnapshotReading.NONE;
        var state=stores.pipelines().snapshotStates(session.original.scope(),claim(session.original),() -> requireSession(session));
        var result=new LinkedHashMap<String,TableSnapshot>();
        boolean boundedLanded=session.bounded && stores.boundedLoads().complete(session.original);
        for(var source:session.sources.values()) {
            var confirmed=session.bounded?List.<String>of():streamingCursor(session,source).snapshotCompletedTables();
            for(String table:source.completed) {
                boolean landed=boundedLanded || confirmed.contains(table);
                var count=state.count(source.id,table,source.snapshotEpoch);
                if(count.isPresent())result.put(session.label(source.id,table),new TableSnapshot(count.getAsLong(),
                        landed?count.getAsLong():null,landed?100:null,landed));
            }
        }
        requireSession(session); return new SnapshotReading(result,session.began);
    }
    private InitialLoad initialLoadReading(Session session, ExecutionPlan plan, Instant at) {
        requireSession(session);
        var original = session.original;
        if (plan == null || !original.pipelineId().equals(plan.pipelineId())
                || !Objects.equals(plan.claimGeneration(), original.claimGeneration())
                || !Objects.equals(plan.executionGeneration(), original.executionGeneration())
                || !Objects.equals(plan.topologyRevision(), original.topologyRevision())) return null;
        var tables = new LinkedHashMap<String, InitialLoad.Table>();
        if (session.snapshot) {
            boolean boundedLanded = session.bounded && stores.boundedLoads().complete(original);
            for (var source : session.sources.values()) {
                var confirmed = session.bounded ? List.<String>of() : streamingCursor(session, source).snapshotCompletedTables();
                for (String table : source.selected.tables()) {
                    Long rows = source.rows.get(table);
                    var stage = boundedLanded || confirmed.contains(table) ? InitialLoad.Stage.LANDED
                            : source.completed.contains(table) ? InitialLoad.Stage.LANDING
                            : rows != null ? InitialLoad.Stage.READING : InitialLoad.Stage.PENDING;
                    tables.put(session.label(source.id, table), new InitialLoad.Table(stage, rows, null));
                }
            }
        }
        requireSession(session);
        return new InitialLoad(InitialLoad.executionKey(plan), at, session.mode.name().toLowerCase(Locale.ROOT), tables);
    }

    private SnapshotReading runSnapshotReading(Session session) {
        requireSession(session); if(!session.snapshot)return SnapshotReading.NONE;
        var result=new LinkedHashMap<String,TableSnapshot>();
        boolean boundedLanded=session.bounded && stores.boundedLoads().complete(session.original);
        for(var source:session.sources.values()) {
            var confirmed=session.bounded?List.<String>of():streamingCursor(session,source).snapshotCompletedTables();
            for(String table:source.selected.tables()) {
                boolean landed=boundedLanded || confirmed.contains(table);
                long rows=source.rows.getOrDefault(table,0L);
                result.put(session.label(source.id,table),new TableSnapshot(rows,landed?rows:null,landed?100:null,landed));
            }
        }
        requireSession(session); return new SnapshotReading(result,session.began);
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
        final AtomicBoolean live=new AtomicBoolean(true);final Owner owner;NativeSrsCaptureRunUnit.Tail tail;final Runnable guard=this::require;
        Physical(Owner owner){this.owner=owner;}
        void require(){owner.require();if(!live.get())throw refused();}
        void close(){live.set(false);if(tail!=null)tail.close();runtime.stopOpenings(guard);}
    }
    private boolean needed(Owner owner) {
        if(closed.get())return false;
        if(owner.sessions.stream().anyMatch(original -> driving.get().test(original.pipelineId())))return true;
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
    private void ensureTail(Owner owner) {
        owner.require();if(owner.config==null || owner.origin==null)throw refused();
        var current=stores.sources().srs(stores.scope(),owner::require);var topology=current.consumers().attached(owner.prepared.chainId());
        var requested=new TreeSet<String>();topology.consumers().forEach(member -> requested.addAll(member.tables()));
        if(requested.isEmpty())return;
        var before=current.chains().load(owner.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        if(owner.physical!=null && !owner.physical.live.get())throw refused();
        if(owner.physical!=null && before.servingTables().containsAll(requested))return;
        var configured=owner.configured;
        if(configured==null)configured=ConfiguredCaptureRun.prepare(owner.origin,owner.config,stores.notes(),connectors,owner::require,stores.recoveryPinned().test(owner.origin.scope()));
        else {stores.notes().guardedBy(owner::require).origin(configured.primary());connectors.resolve(configured.connectorId(),configured.artifactHash());owner.require();}
        if(owner.physical!=null){owner.physical.close();owner.physical=null;before=current.chains().load(owner.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);}
        if(before.epoch()>0 && before.checkpoint()==null)throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN,Map.of("chain",owner.prepared.chainId()),null);
        before=current.chains().requestTables(owner.prepared.chainId(),requested,owner.fence);
        long epoch=current.chains().openEpoch(owner.prepared.chainId(),before.epoch(),owner.fence).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        if(!current.chains().publishServingTables(owner.prepared.chainId(),epoch,before.requestedTables(),owner.fence))throw refused();
        var opened=current.chains().load(owner.prepared.chainId()).orElseThrow(ScopedPipelineCaptureCoordinator::refused);
        owner.configured=configured;
        startPreparedTail(owner,opened);
    }
    private void startPreparedTail(Owner owner,SrsChainState opened) {
        owner.require();if(owner.physical!=null || owner.configured==null || !opened.miningChainId().equals(owner.prepared.chainId()))throw refused();
        var config=new CaptureConfig(owner.config.connectorId(),owner.config.settings(),opened.servingTables().stream().sorted().toList(),owner.config.node(),owner.config.sharedNotes());
        var start=opened.resumeToken()==null?CaptureStart.present():CaptureStart.resume(new SourcePosition(opened.resumeToken()));
        var physical=new Physical(owner);owner.physical=physical;var notes=owner.configured;
        try {physical.tail=runtime.startTail(stores.scope(),opened,owner.fence,config,start,writer -> ports.open(notes,config,physical.guard,null,writer::prepare),physical.guard,owner.positionEditsRestricted);}
        catch(RuntimeException|Error failure) {
            try {physical.close();owner.physical=null;}catch(RuntimeException|Error cleanup){if(cleanup!=failure)failure.addSuppressed(cleanup);}
            throw failure;
        }
    }
    private RingNotOpenYet awaiting(Source source) {
        var key=source.consumer.context().key();long now=System.nanoTime();long began=waiting.computeIfAbsent(key,ignored -> now);
        var ttl=execution.ownership().ttl();if(now-began>=ttl.toNanos())throw new TapstateException(CaptureError.NO_RING_TO_ATTACH,
                Map.of("captureId",source.prepared.captureId(),"seconds",ttl.toSeconds()),null);
        return new RingNotOpenYet(new CaptureId(source.prepared.captureId()));
    }
    private synchronized void maintain() {
        if(closed.get())return;
        for(var owner:List.copyOf(owners.values()))if(owner.prepared.mode()==CapturePreparation.Mode.SHARED_SRS && !owner.ended.get()) {
            try {owner.require();if(owner.physical!=null)owner.physical.tail.health().failure().ifPresent(failure -> {throwFailure(failure);});ensureTail(owner);retain(owner);}
            catch(RuntimeException|Error failure) {
                for(var session:sessions.values())if(session.sources.values().stream().anyMatch(source -> source.owner==owner))session.failure.compareAndSet(null,failure);
                owner.ended.set(true);try {owner.release();owners.remove(owner.prepared.captureId(),owner);}
                catch(RuntimeException|Error ending){if(ending!=failure)failure.addSuppressed(ending);}
            }
        }
        for(var session:List.copyOf(sessions.values()))if(session.failure.get()==null && !session.ended.get())for(var source:session.sources.values())if(source.shared && (source.owner==null || source.owner.ended.get())) {
            try {
                requireSession(session);
                if(source.foreign!=null)try {execution.authorization().requireCapture(stores.scope(),source.foreign);continue;}catch(TapstateException expired) { /* Re-read this same prepared capture before any takeover. */ }
                var prepared=stores.preparations().load(source.prepared);source.owner=owner(session,source,prepared);
                if(source.owner!=null){source.owner.positionEditsRestricted|=source.positionEditsRestricted;source.owner.origin=session.original;source.owner.config=source.selected.config().at(new PipelineNode(session.original.pipelineId(),source.id)).sharing(new SharedNotes(source.prepared.canonicalChainId(),List.of()));ensureTail(source.owner);}
            }catch(RuntimeException|Error failure){session.failure.compareAndSet(null,failure);}
        }
    }
    private void retain(Owner owner) {
        try { runtime.retain(stores.scope(),owner.prepared.chainId(),owner.fence,owner::require); }
        catch (TapstateException unavailable) {
            if (unavailable.code()!=IoError.STORE_UNAVAILABLE) throw unavailable;
            // Cleanup can retry from current confirmed floors; capture and ownership failures remain fatal.
            owner.require();
            LOG.warn("Retention for capture {} was deferred [{}]",owner.prepared.captureId(),unavailable.code().code());
        }
    }
    private static void throwFailure(Throwable failure){if(failure instanceof RuntimeException runtime)throw runtime;if(failure instanceof Error error)throw error;throw new IllegalStateException("capture failed",failure);}
    private void lost(Owner owner) {
        owner.ended.set(true);Throwable failure=null;
        synchronized(this) {
            if(owner.physical!=null) {owner.physical.close();owner.physical=null;}
            for(var session:sessions.values())for(var source:session.sources.values())if(source.owner==owner && !source.shared) {
                session.failure.compareAndSet(null,refused());if(source.run!=null)failure=cleanup(failure,() -> runtime.stopSnapshot(source.run));
            }
            rethrow(failure);
            // An exited owner's release remains retryable even when its renewer closed on loss.
            owner.release();
            for(var session:sessions.values())for(var source:session.sources.values())if(source.owner==owner && source.shared) {
                source.owner=null;source.foreign=owner.fence;
            }
            owners.remove(owner.prepared.captureId(),owner);
        }
        rethrow(failure);
    }
    private void stop(Session session) {
        session.ended.set(true); Throwable first=null;
        stores.boundedLoads().forget(session.original);
        for(var source:session.sources.values()) {
            if(source.run!=null)first=cleanup(first,() -> runtime.stopSnapshot(source.run));
            if(source.guard!=null)first=cleanup(first,() -> runtime.stopOpenings(source.guard));
            if(session.bindings!=null)first=cleanup(first,() -> buffer.release(PipelineCaptureBindings.bufferKey(session.original,source.id)));
        }
        rethrow(first);
        var unique=Collections.newSetFromMap(new IdentityHashMap<Owner,Boolean>()); session.sources.values().forEach(source -> { if(source.owner!=null)unique.add(source.owner); });
        for(var owner:unique) {
            owner.sessions.remove(session.original);
            if(owner.sessions.isEmpty() && (owner.ended.get() || !needed(owner))) {
                owner.ended.set(true);owner.release();owners.remove(owner.prepared.captureId(),owner);
            }
        }
        rethrow(first);
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
    private void requireSession(Session session) { active(); if(session.ended.get())throw refused(); requirePipeline(session.original); }
    private void requirePipeline(ExecutionFence original) { active(); if(!claim(original).equals(execution.authorization().require(original)))throw refused(); }
    private void active() { execution.active().run(); if(closed.get())throw refused(); }
    private static WorkloadClaimFence claim(ExecutionFence original) {
        return new WorkloadClaimFence(new WorkloadClaimKey(original.scope().clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,original.pipelineId()),
                new WorkloadOwner(original.ownerNodeId(),original.ownerBootId()),original.claimGeneration(),original.executionGeneration(),original.topologyRevision());
    }
    private static CacheBusinessScope business(PipelineIntentScope scope) { return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()); }
    private static TapstateException refused() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null); }
    @Override public synchronized void close() {
        closed.set(true);maintenance.shutdownNow(); Throwable first=null;
        owners.values().forEach(owner -> owner.ended.set(true));
        for(var session:List.copyOf(sessions.values())) {
            try {stop(session);sessions.remove(session.original,session);}
            catch(RuntimeException|Error failure){if(first==null)first=failure;else if(first!=failure)first.addSuppressed(failure);}
        }
        for(var owner:List.copyOf(owners.values())) {
            if(sessions.values().stream().anyMatch(session -> session.sources.values().stream().anyMatch(source -> source.owner==owner)))continue;
            try {owner.release();owners.remove(owner.prepared.captureId(),owner);}
            catch(RuntimeException|Error failure){if(first==null)first=failure;else if(first!=failure)first.addSuppressed(failure);}
        }
        rethrow(first);
    }
}
