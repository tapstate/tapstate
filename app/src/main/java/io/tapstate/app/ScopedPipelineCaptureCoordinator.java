package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.runtime.srs.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Native source owners retain the original Pipeline, capture and physical handoff identities. */
final class ScopedPipelineCaptureCoordinator implements PipelineCaptureCoordinator,AutoCloseable {
    record Stores(CacheBusinessScope scope,PipelineStoreViews pipelines,SrsStoreViews sources,
            CaptureStatePreparations configuration,CaptureCachePreparations preparations,RequiredNoteStore notes) {
        Stores { Objects.requireNonNull(scope); Objects.requireNonNull(pipelines); Objects.requireNonNull(sources); Objects.requireNonNull(configuration); Objects.requireNonNull(preparations); Objects.requireNonNull(notes); }
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
                io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer preparation);
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

    ScopedPipelineCaptureCoordinator(Stores stores,Execution execution,NativeSrsCaptureRunUnit runtime,
            SnapshotBuffer buffer,ConnectorProvisioner connectors) {
        this(stores,execution,runtime,buffer,connectors,(original,config,guard,preparation) -> {
            var port=original.port(config,stores.notes(),connectors,guard);
            return preparation==null?port:port.withSnapshotPreparation(preparation);
        });
    }
    ScopedPipelineCaptureCoordinator(Stores stores,Execution execution,NativeSrsCaptureRunUnit runtime,
            SnapshotBuffer buffer,ConnectorProvisioner connectors,Ports ports) {
        this.stores=Objects.requireNonNull(stores); this.execution=Objects.requireNonNull(execution);
        this.runtime=Objects.requireNonNull(runtime); this.buffer=Objects.requireNonNull(buffer);
        this.connectors=Objects.requireNonNull(connectors); this.ports=Objects.requireNonNull(ports);
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
            synchronized (ScopedPipelineCaptureCoordinator.this) { var session=sessions.remove(original); if(session!=null) stop(session); }
        }
        @Override public PipelineCaptureBindings captureBindings() { return session().bindings; }
        @Override public boolean isCapturing(String pipeline) { requireId(pipeline); synchronized(ScopedPipelineCaptureCoordinator.this) { return sessions.containsKey(original); } }
        @Override public Optional<Throwable> captureFailure(String pipeline) {
            requireId(pipeline); synchronized(ScopedPipelineCaptureCoordinator.this) {
                var held=sessions.get(original); if(held==null)return Optional.empty();
                if(held.failure.get()!=null)return Optional.of(held.failure.get());
                return held.sources.values().stream().filter(source -> source.run!=null).map(source -> source.run.failure()).flatMap(Optional::stream).findFirst();
            }
        }
        @Override public boolean loadDelivered(String pipeline) { requireId(pipeline); var held=current(); if(held==null || !held.snapshot || held.sources.isEmpty())return true;
            requireSession(held);if(held.bounded)return false;
            return held.sources.values().stream().allMatch(source -> source.channels.load(source.direct.context()).cursor().snapshotCompletedTables().containsAll(source.selected.tables())); }
        @Override public SnapshotReading snapshotProgress(String pipeline) { requireId(pipeline); var held=current(); return held==null?SnapshotReading.NONE:snapshotReading(held); }
        @Override public SnapshotReading runSnapshotProgress(String pipeline) { return snapshotProgress(pipeline); }
        @Override public CaptureReading capturedRows(String pipeline) {
            requireId(pipeline); var held=current(); if(held==null)return CaptureReading.NONE; var rows=new LinkedHashMap<String,Map<String,Long>>(); var bytes=new LinkedHashMap<String,Long>();
            for(var source:held.sources.values()) if(source.run!=null) {
                source.run.health().receivedRows().forEach((table,counts) -> rows.put(held.label(source.id,table),counts));
                source.run.health().receivedBytes().forEach((table,count) -> bytes.put(held.label(source.id,table),count));
            }
            return new CaptureReading(rows,bytes,held.began);
        }
        private Session current() { synchronized(ScopedPipelineCaptureCoordinator.this) { return sessions.get(original); } }
        private Session session() { var session=current(); if(session==null || session.bindings==null)throw refused(); return session; }
        private void requireId(String pipeline) { if(!original.pipelineId().equals(pipeline))throw refused(); }
    }
    private synchronized void start(Run run,ArtifactStore artifacts,SourceModelSnapshot models) {
        active(); requirePipeline(run.original); Objects.requireNonNull(artifacts); Objects.requireNonNull(models).requirePipeline(run.original.pipelineId());
        var pipeline=StoredArtifacts.requirePipeline(artifacts,run.original.pipelineId());
        var sourceDefinitions=new LinkedHashMap<String,SourceResource>();
        for(String id:pipeline.sourceIds()) sourceDefinitions.put(id,StoredArtifacts.requireSource(artifacts,id));
        var inputs=models.preparationInputs(run.original.scope(),pipeline,pipeline,sourceDefinitions);
        var existing=sessions.get(run.original);
        if(existing!=null) { if(!existing.inputs.equals(inputs))throw refused(); return; }
        var mode=pipeline.settings()==null || pipeline.settings().readMode()==null?ReadMode.SNAPSHOT_AND_CDC:pipeline.settings().readMode();
        if(mode!=ReadMode.SNAPSHOT_ONLY && (inputs.directChannels().size()!=inputs.selectedTables().values().stream().filter(tables -> !tables.isEmpty()).count()))throw refused();
        var session=new Session(run.original,inputs,execution.clock().instant(),mode); sessions.put(run.original,session);
        try {
            var node=execution.node().get().orElseThrow(ScopedPipelineCaptureCoordinator::refused);
            if(!node.owner().equals(claim(run.original).owner()))throw refused();
            for(var definition:sourceDefinitions.values()) {
                var selected=SourceCaptureResolution.forPipeline(pipeline,definition,models.model(definition));
                if(selected.isEmpty())continue;
                var source=new Source(definition.id(),selected.orElseThrow()); session.sources.put(source.id,source);
                var request=new CapturePreparationRequest(run.original.scope(),claim(run.original),node,source.id,inputs);
                var issued=stores.configuration().issueFirstCreate(request);
                var prepared=issued instanceof CaptureCreationResult.Issued first?stores.preparations().createFirst(first.creation()):
                        stores.preparations().load(((CaptureCreationResult.AlreadyIssued)issued).preparation());
                if(prepared.preparation().mode()!=(session.bounded?CapturePreparation.Mode.SNAPSHOT_ONLY:CapturePreparation.Mode.DIRECT))throw refused();
                source.owner=owner(session,source,prepared);
            }
            var snapshots=new LinkedHashMap<String,PipelineCaptureBindings.SnapshotOnlySource>();
            var state=session.snapshot?stores.pipelines().snapshotStates(run.original.scope(),claim(run.original),() -> requireSession(session)):null;
            long prior=state==null?0:state.order(); long epoch=0;
            if(session.bounded && !session.sources.isEmpty()) {
                var captures=new LinkedHashMap<String,CaptureCacheState>();
                session.sources.forEach((id,source) -> captures.put(id,new CaptureCacheState(source.owner.prepared,source.owner.fence,Optional.empty())));
                epoch=state.advanceOrder(prior,prior,captures);
                for(var source:session.sources.values())snapshots.put(source.id,new PipelineCaptureBindings.SnapshotOnlySource(source.selected.chainId().value(),Set.copyOf(source.selected.tables()),epoch));
            }
            var direct=new LinkedHashMap<String,DirectSourceRun>();
            if(!session.bounded)for(var source:session.sources.values()) {
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
                String token=cursor.sinkAcked()!=null?cursor.sinkAcked().token():cursor.cdcStartPosition();
                if(token==null)token=source.direct.current().anchor();
                source.start=token==null?sourceStart(pipeline):CaptureStart.resume(new SourcePosition(token));
            }
            // The preparation mode belongs to the state generation; its READY receipt suppresses repeated target clearing.
            session.bindings=new PipelineCaptureBindings(run.original,Map.of(),direct,snapshots,session.snapshot && !session.sources.isEmpty());
            for(var source:session.sources.values()) {source.snapshotEpoch=epoch;source.readTables=Set.copyOf(source.selected.tables());}
            for(var source:session.sources.values()) {
                String handoff=session.bindings.bufferKey(source.id);
                var declared=new LinkedHashMap<String,CaptureHandoff>();
                for(String table:source.selected.tables()) {
                    String ring=source.selected.ringName(table); if(session.snapshot)buffer.declareSnapshot(handoff,ring);
                    declared.put(table,buffer.handoff(handoff,ring));
                }
                var originalHandoffs=Map.copyOf(declared);
                Runnable guard=() -> { requireSession(session); source.owner.require(); };
                var config=source.selected.config().at(new PipelineNode(run.original.pipelineId(),source.id));
                var notes=ConfiguredCaptureRun.prepare(run.original,config,stores.notes(),connectors,guard);
                CaptureHandoff output=new CaptureHandoff() {
                    @Override public void accept(Envelope row) {
                        guard.run(); if(!source.selected.tables().contains(row.src()))throw refused();
                        originalHandoffs.get(row.src()).accept(row); source.rows.merge(row.src(),1L,Long::sum);
                    }
                    @Override public void loaded(String table) {
                        guard.run(); if(!session.snapshot || !source.selected.tables().contains(table))throw refused();
                        if(source.readTables.contains(table))state.recordCount(source.id,table,source.snapshotEpoch,source.rows.getOrDefault(table,0L));
                        guard.run(); source.completed.add(table); originalHandoffs.get(table).loaded(table);
                    }
                };
                if(!session.bounded && session.snapshot) {
                    source.run=runtime.beginDirectSnapshot(config,source.direct,source.channels,preparation -> {
                        source.snapshotEpoch=preparation.request().snapshotEpoch();source.readTables=Set.copyOf(preparation.stillOwed());
                        return ports.open(notes,config,guard,preparation::prepare);
                    },output,buffer.capacity(),guard);
                    source.direct=source.channels.load(source.direct.context());direct.put(source.id,DirectSourceRun.of(run.original,source.direct));
                } else {
                    var port=ports.open(notes,config,guard,null);
                    source.run=session.bounded?runtime.beginSnapshotOnly(config,epoch,port,output,guard):
                            runtime.startDirect(config,source.direct,source.channels,source.start,port,output,buffer.capacity(),guard);
                }
                guard.run();
            }
            session.bindings=new PipelineCaptureBindings(run.original,Map.of(),direct,snapshots,session.snapshot && !session.sources.isEmpty());
            requireSession(session);
        } catch(RuntimeException|Error failure) {
            sessions.remove(run.original); try { stop(session); } catch(RuntimeException|Error cleanup) { if(cleanup!=failure)failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private Owner owner(Session session,Source source,CaptureCacheState prepared) {
        var id=prepared.preparation().captureId(); var existing=owners.get(id);
        if(existing!=null && !existing.ended.get()) {
            if(!existing.prepared.equals(prepared.preparation()))throw refused(); source.owner=existing; existing.require();
            if(!existing.lease.retain(() -> existing.sessions.add(session.original)))throw refused(); return existing;
        }
        var permit=execution.ownership().acquire(new CaptureId(id));
        if(!permit.acquired())throw new RingNotOpenYet(new CaptureId(id));
        if(permit.fence()==null)throw refused();
        var owner=new Owner(prepared.preparation(),permit.claim()); owners.put(id,owner); owner.sessions.add(session.original); source.owner=owner;
        owner.lease=new CaptureClaimLease(execution.ownership(),permit.claim(),execution.renewal(),() -> lost(owner));
        owner.lease.whileNeeded(() -> owner.sessions.stream().anyMatch(original -> driving.get().test(original.pipelineId())));
        owner.require(); return owner;
    }
    private final class Owner {
        final CapturePreparation prepared; final WorkloadClaimFence fence; final WorkloadClaim initial;
        final Set<ExecutionFence> sessions=ConcurrentHashMap.newKeySet(); final AtomicBoolean ended=new AtomicBoolean();
        CaptureClaimLease lease;
        Owner(CapturePreparation prepared,WorkloadClaim initial) { this.prepared=prepared;this.initial=initial;this.fence=WorkloadClaimFence.from(initial); }
        void release() { if(lease!=null)lease.close();else execution.ownership().release(initial); }
        void require() { active(); if(ended.get() || !fence.equals(execution.authorization().requireCapture(stores.scope(),fence)))throw refused(); }
    }
    private static final class Source {
        final String id; final SourceCaptureResolution selected; final Map<String,Long> rows=new ConcurrentHashMap<>();
        final Set<String> completed=ConcurrentHashMap.newKeySet();
        Owner owner; CaptureRun run; DirectChannelStore channels; DirectChannelProgress direct; CaptureStart start; long snapshotEpoch; Set<String> readTables=Set.of();
        Source(String id,SourceCaptureResolution selected) { this.id=id;this.selected=selected; }
    }
    private static final class Session {
        final ExecutionFence original; final PipelinePreparationInputs inputs; final Instant began; final boolean bounded,snapshot;
        final Map<String,Source> sources=new LinkedHashMap<>(); final AtomicBoolean ended=new AtomicBoolean(); final AtomicReference<Throwable> failure=new AtomicReference<>();
        PipelineCaptureBindings bindings;
        Session(ExecutionFence original,PipelinePreparationInputs inputs,Instant began,ReadMode mode) { this.original=original;this.inputs=inputs;this.began=began;this.bounded=mode==ReadMode.SNAPSHOT_ONLY;this.snapshot=mode!=ReadMode.CDC_ONLY; }
        String label(String source,String table) {
            return sources.values().stream().filter(item -> item.selected.tables().contains(table)).count()>1?source+"."+table:table;
        }
    }
    private SnapshotReading snapshotReading(Session session) {
        requireSession(session); if(!session.snapshot)return SnapshotReading.NONE;
        var state=stores.pipelines().snapshotStates(session.original.scope(),claim(session.original),() -> requireSession(session));
        var result=new LinkedHashMap<String,TableSnapshot>();
        for(var source:session.sources.values())for(String table:source.completed) {
            var count=state.count(source.id,table,source.snapshotEpoch);
            if(count.isPresent())result.put(session.label(source.id,table),new TableSnapshot(count.getAsLong(),null,null,false));
        }
        return new SnapshotReading(result,session.began);
    }
    private void lost(Owner owner) {
        owner.ended.set(true);
        synchronized(this) {
            for(var original:owner.sessions) {
                var session=sessions.get(original); if(session!=null) { session.failure.compareAndSet(null,refused()); for(var source:session.sources.values())if(source.owner==owner && source.run!=null)runtime.stopSnapshot(source.run); }
            }
            owners.remove(owner.prepared.captureId(),owner);
        }
    }
    private void stop(Session session) {
        if(!session.ended.compareAndSet(false,true))return; Throwable first=null;
        for(var source:session.sources.values()) {
            if(source.run!=null)first=cleanup(first,() -> runtime.stopSnapshot(source.run));
            if(session.bindings!=null)first=cleanup(first,() -> buffer.release(session.bindings.bufferKey(source.id)));
        }
        var unique=Collections.newSetFromMap(new IdentityHashMap<Owner,Boolean>()); session.sources.values().forEach(source -> { if(source.owner!=null)unique.add(source.owner); });
        for(var owner:unique) {
            owner.sessions.remove(session.original);
            if(owner.sessions.isEmpty() && owner.ended.compareAndSet(false,true)) {
                owners.remove(owner.prepared.captureId(),owner); first=cleanup(first,owner::release);
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
    private static CaptureStart sourceStart(PipelineResource pipeline) {
        var settings=pipeline.settings();var start=StartFrom.parse(settings==null || settings.startFrom()==null?"latest":settings.startFrom());
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
        if(!closed.compareAndSet(false,true))return; Throwable first=null;
        for(var session:List.copyOf(sessions.values()))first=cleanup(first,() -> stop(session));
        sessions.clear();
        for(var owner:List.copyOf(owners.values())) { owner.ended.set(true); first=cleanup(first,owner::release); }
        owners.clear(); rethrow(first);
    }
}
