package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.function.BooleanSupplier;
import io.tapstate.core.model.*;
import io.tapstate.core.lifecycle.StateJson;

/** Executes only the original explicit Pipeline full and observes actual sink completion under its run fence. */
final class NativePipelineRecovery {
    private final MongoConfigurationDomain configuration;
    private final CacheAuthScope auth;
    private final PipelineRecoveryInspector inspector;
    private final PipelineFirstStatePreparer preparer;
    private final ConnectorProvisioner connectors;
    private final Engine engine;
    private final PipelineCaptureCoordinator captures;
    private final ExecutionAuthorization authorization;
    private final Runnable active;
    private final BooleanSupplier available;
    private final StorePort store;
    private final PipelineStoreViews views;

    NativePipelineRecovery(MongoConfigurationDomain configuration,CacheAuthScope auth,StorePort store,
            PipelineFirstStatePreparer preparer,ConnectorProvisioner connectors,Engine engine,PipelineCaptureCoordinator captures,
            ExecutionAuthorization authorization,Runnable active,BooleanSupplier available,PipelineStoreViews views) {
        this.configuration=Objects.requireNonNull(configuration);this.auth=Objects.requireNonNull(auth);
        this.preparer=Objects.requireNonNull(preparer);this.engine=Objects.requireNonNull(engine);this.captures=Objects.requireNonNull(captures);
        this.authorization=Objects.requireNonNull(authorization);this.active=Objects.requireNonNull(active);this.available=Objects.requireNonNull(available);
        this.store=Objects.requireNonNull(store);this.views=Objects.requireNonNull(views);
        this.connectors=Objects.requireNonNull(connectors);
        this.inspector=new PipelineRecoveryInspector(configuration,store,connectors,this::requireAvailable);
    }
    PipelineRecoveryInspector.Inspection inspect(StorageRecoveryCommands.Selection selection) {
        requireAvailable();return inspectOperatorDatabases(inspector.inspect(auth,selection));
    }
    PipelineRecoveryInspector.Inspection inspectCurrent(StorageRecoveryCommands.Selection selection) {
        requireAvailable();return inspector.inspectCurrent(auth,selection);
    }
    PipelineRecoveryInspector.Inspection verifyExternal(StorageRecoveryCommands.Selection selection) {
        requireAvailable();return inspectOperatorDatabases(inspector.inspectContinuation(auth,selection));
    }
    private PipelineRecoveryInspector.Inspection inspectOperatorDatabases(PipelineRecoveryInspector.Inspection inspected) {
        if(!inspected.operatorDatabases().isEmpty()) {
            if(!(views instanceof io.tapstate.adapters.mongostore.MongoCacheDomain cache))throw fenced();
            String id=inspected.manifest().selection().pipelineIds().iterator().next();
            var summary=configuration.cacheRecoveries().read(id).orElseThrow(NativePipelineRecovery::fenced);
            for(String database:inspected.operatorDatabases())cache.inspectRecoveredOperatorDatabase(summary,database,this::requireAvailable);
        }
        return inspected;
    }
    String failureCode(StorageRecoveryCommands.Plan plan) {
        requireAvailable();
        if(plan.phase()!=StorageRecoveryCommands.Phase.RUNNING || !plan.originalScope().equals(auth))return null;
        String id=plan.manifest().selection().pipelineIds().iterator().next();
        var summary=configuration.cacheRecoveries().read(id).orElseThrow(NativePipelineRecovery::fenced);
        if(configuration.preparations().read(id).orElseThrow(NativePipelineRecovery::fenced).phase()!=PipelinePreparation.Phase.COMPLETE)return null;
        var actual=views.pipelineStates(summary.targetScope()).read(id);
        if(actual.isEmpty() || StateJson.parse(actual.orElseThrow().stateJson())!=PipelineState.FAILED)return null;
        return views.pipelineObservations(summary.targetScope()).read(id).map(io.tapstate.core.lifecycle.Observation::failure)
                .map(io.tapstate.core.lifecycle.ObservationFailure::code).orElse(io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE.code());
    }
    Map<String,String> inspectCaptured(PipelineConvergencePass pass,ReadOnlyArtifactSnapshot snapshot,SourceModelSnapshot models) {
        requireAvailable();var summary=configuration.cacheRecoveries().read(pass.scope().pipelineId());
        if(summary.isEmpty())return Map.of();
        if(!summary.orElseThrow().targetScope().equals(pass.scope()))throw fenced();
        String id=pass.scope().pipelineId();var pipeline=PipelineInlining.inline(StoredArtifacts.requirePipeline(snapshot,id),snapshot);
        var contexts=new LinkedHashMap<String,SourceResource>();
        for(String source:pipeline.sourceIds())contexts.put(RequiredNoteOwner.namespace(new PipelineNode(id,source),PipelineNode.Purpose.CAPTURE),StoredArtifacts.requireSource(snapshot,source));
        if(pipeline.serve() instanceof ServeBlock.Inline serve && serve.sync()!=null)for(var sync:serve.sync())
            contexts.put(RequiredNoteOwner.namespace(new PipelineNode(id,PipelineConnectorNodes.syncNodeId(sync)),PipelineNode.Purpose.SYNC),StoredArtifacts.requireSource(snapshot,sync.source()));
        if(pipeline.view() instanceof ViewBlock.Inline view) {
            String managed=snapshot.managedDefaultSourceId().orElseThrow(NativePipelineRecovery::fenced);
            contexts.put(RequiredNoteOwner.namespace(new PipelineNode(id,view.id()),PipelineNode.Purpose.VIEW),StoredArtifacts.requireSource(snapshot,ViewTargetResolver.resolve(view,managed).sourceId()));
        }
        var hashes=new TreeMap<String,String>();var notes=configuration.requiredNotes().guardedBy(this::requireAvailable);
        for(var entry:contexts.entrySet()) {
            var token=notes.open(new RequiredNoteOwner(auth.bindingVersion(),entry.getKey(),RequiredNoteOwner.Kind.PIPELINE,id,pass.scope().stateGeneration()));
            var origin=notes.origin(token);var source=entry.getValue();
            if(!origin.equals(ConnectorNoteOrigin.capture(source.connector(),origin.artifactHash(),source.config())))throw fenced();
            String prior=hashes.putIfAbsent(source.connector(),origin.artifactHash());if(prior!=null && !prior.equals(origin.artifactHash()))throw fenced();
        }
        ConnectorProvisioner pinned=fixed(hashes);
        var pending=configuration.recoveryCommands().current(auth);
        if(pending.isPresent() && pending.orElseThrow().manifest().selection().extent()==StorageRecoveryCommands.Extent.PIPELINE
                && pending.orElseThrow().manifest().selection().pipelineIds().contains(id)
                && pending.orElseThrow().phase()!=StorageRecoveryCommands.Phase.COMPLETE) {
            var plan=pending.orElseThrow();if(plan.phase()!=StorageRecoveryCommands.Phase.RUNNING)throw fenced();
            var inspected=new PipelineRecoveryInspector(configuration,store,pinned,this::requireAvailable).inspectCaptured(auth,plan.manifest().selection(),snapshot,models);
            requireSame(plan,inspected);configuration.cacheRecoveries().authorizeFull(plan,id).requireActive();
        }
        pass.requirePipeline(id);return Map.copyOf(hashes);
    }
    private ConnectorProvisioner fixed(Map<String,String> hashes) {
        var captured=Map.copyOf(hashes);
        return new ConnectorProvisioner() {
            @Override public ConnectorRef resolve(String connector) {
                var hash=captured.get(connector);if(hash==null)throw fenced();return connectors.resolve(connector,hash);
            }
            @Override public ConnectorRef resolve(String connector,String hash){return connectors.resolve(connector,hash);}
        };
    }
    long acceptedIntentRevision(StorageRecoveryCommands.Plan plan) {
        requireAvailable();
        if(!plan.originalScope().equals(auth) || plan.manifest().selection().extent()!=StorageRecoveryCommands.Extent.PIPELINE
                || plan.manifest().selection().pipelineIds().size()!=1)throw fenced();
        String id=plan.manifest().selection().pipelineIds().iterator().next();
        var summary=configuration.cacheRecoveries().read(id).orElseThrow(NativePipelineRecovery::fenced);
        var intent=configuration.intents().load(id).orElseThrow(NativePipelineRecovery::fenced);
        if(intent.deleted() || !intent.scope().equals(summary.targetScope()))throw fenced();
        return intent.intentRevision();
    }
    void start(StorageRecoveryCommands.Plan requested,boolean explicitContinuation,long acceptedRevision) {
        requireAvailable();var commands=configuration.recoveryCommands();
        var current=commands.find(auth,requested.operationId()).orElseThrow(NativePipelineRecovery::fenced);
        if(!current.originalScope().equals(auth) || !current.planHash().equals(requested.planHash()) || current.controlRevision()!=requested.controlRevision())throw fenced();
        if(current.phase()==StorageRecoveryCommands.Phase.COMPLETE)return;
        if(current.phase()==StorageRecoveryCommands.Phase.RUNNING && !explicitContinuation)return;
        if(current.phase()!=StorageRecoveryCommands.Phase.CONFIRMED && current.phase()!=StorageRecoveryCommands.Phase.RUNNING)throw fenced();
        if(acceptedIntentRevision(current)!=acceptedRevision)throw fenced();
        var inspected=current.phase()==StorageRecoveryCommands.Phase.CONFIRMED?inspector.inspect(auth,current.manifest().selection()):inspector.inspectContinuation(auth,current.manifest().selection());
        requireSame(current,inspected);if(inspected.intentRevision()!=acceptedRevision)throw fenced();
        inspectOperatorDatabases(inspected);
        var running=current.phase()==StorageRecoveryCommands.Phase.CONFIRMED?commands.guardedBy(this::requireAvailable).start(current):current;
        String id=running.manifest().selection().pipelineIds().iterator().next();
        var permission=configuration.cacheRecoveries().guardedBy(this::requireAvailable).authorizeFull(running,id);
        ConnectorProvisioner fixed=fixed(inspected.connectorHashes());
        var notes=new PipelinePrivateNoteRetirement(configuration.requiredNotes(),configuration.rebuilds(),fixed);
        preparer.prepareRecovered(permission,inspected.inputs(),() -> {
            requireAvailable();
            if(engine.captureRun(id).filter(Engine.Run::isCarryingAJob).isPresent())throw fenced();
            notes.reconcileRecovered(permission,inspected.privateContexts(),this::requireAvailable);
        });
        requireAvailable();permission.requireActive();
        if(!inspected.operatorDatabases().isEmpty()) {
            if(!(views instanceof io.tapstate.adapters.mongostore.MongoCacheDomain cache))throw fenced();
            var node=preparer.requireCurrentNode();
            for(String database:inspected.operatorDatabases())cache.prepareRecoveredOperatorDatabase(permission,node,database,this::requireAvailable);
        }
        var target=permission.recovery().targetScope();
        var configuredNotes=configuration.requiredNotes().guardedBy(() -> {requireAvailable();permission.requireActive();});
        for(var entry:inspected.privateContexts().entrySet()) {
            var token=configuredNotes.open(new RequiredNoteOwner(target.bindingVersion(),entry.getKey(),RequiredNoteOwner.Kind.PIPELINE,id,target.stateGeneration()));
            var source=entry.getValue();var expected=ConnectorNoteOrigin.capture(source.connector(),fixed.resolve(source.connector()).contentHash(),source.config());
            configuredNotes.prepareOrigin(token,expected);
        }
        var intent=configuration.intents().load(id).orElseThrow(NativePipelineRecovery::fenced);
        if(intent.deleted() || intent.intentRevision()!=inspected.intentRevision() || !intent.scope().equals(permission.recovery().targetScope()))throw fenced();
        if(intent.desired().targetState()!=PipelineState.RUNNING && !configuration.intents().guardedBy(this::requireAvailable)
                .startRecovered(permission,inspected.intentRevision(),inspected.inputs().pipelineHash()))throw fenced();
    }
    @org.springframework.scheduling.annotation.Scheduled(fixedDelay=1000)
    void observeCompletion() {
        if(!available.getAsBoolean())return;
        try {
            active.run();var commands=configuration.recoveryCommands();var pending=commands.current(auth);
            if(pending.isEmpty())return;var plan=pending.orElseThrow();
            if(!plan.originalScope().equals(auth) || plan.phase()!=StorageRecoveryCommands.Phase.RUNNING
                    || plan.manifest().selection().extent()!=StorageRecoveryCommands.Extent.PIPELINE || plan.manifest().selection().pipelineIds().size()!=1)return;
            String id=plan.manifest().selection().pipelineIds().iterator().next();
            var summary=configuration.cacheRecoveries().read(id).orElseThrow(NativePipelineRecovery::fenced);
            var permission=configuration.cacheRecoveries().authorizeFull(plan,id);permission.requireActive();
            var intent=configuration.intents().load(id).orElseThrow(NativePipelineRecovery::fenced);
            if(intent.deleted() || !intent.scope().equals(summary.targetScope()) || intent.desired().targetState()!=PipelineState.RUNNING)return;
            var checkpoint=views.pipelineStates(summary.targetScope()).read(id);
            if(checkpoint.isPresent() && StateJson.parse(checkpoint.orElseThrow().stateJson())==PipelineState.COMPLETED) {
                // This is the new scope's normal sink-verified terminal fact, never a reconstructed old position.
                commands.guardedBy(() -> {requireAvailable();permission.requireActive();}).complete(plan);return;
            }
            var run=engine.captureRun(id);
            if(run.isEmpty() || !run.orElseThrow().isCarryingAJob() || run.orElseThrow().failure().isPresent())return;
            if(!(run.orElseThrow().operatorManifest().executionIdentity() instanceof ExecutionFence fence) || fence.scope()==null || !fence.scope().equals(summary.targetScope()))throw fenced();
            var claim=authorization.require(fence);var capture=captures.forRun(summary.targetScope(),claim,fence);
            if(!capture.isCapturing(id) || capture.captureFailure(id).isPresent())return;
            var bindings=capture.captureBindings();
            if(!bindings.freshFullLoad() || !bindings.original().equals(fence))return;
            if(!bindings.snapshots().isEmpty() || !capture.loadDelivered(id))return;
            permission.requireActive();
            // The real target evidence is already fixed. The CONFIG callback uses only the captured local proof.
            commands.guardedBy(() -> {requireAvailable();permission.requireActive();authorization.requireLocal(fence);}).complete(plan);
        } catch(TapstateException unavailable) {
            // Missing or changed evidence keeps the original operation incomplete; it never starts another run.
        }
    }
    private static void requireSame(StorageRecoveryCommands.Plan plan,PipelineRecoveryInspector.Inspection inspected) {
        if(!plan.manifest().definitionHashes().equals(inspected.manifest().definitionHashes())
                || !plan.manifest().dependencyHash().equals(inspected.manifest().dependencyHash())
                || !plan.manifest().affectedPipelineIds().equals(inspected.manifest().affectedPipelineIds()))throw fenced();
    }
    private void requireAvailable(){active.run();if(!available.getAsBoolean())throw fenced();}
    private static TapstateException fenced(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
