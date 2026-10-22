package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.adapters.pdk.PdkCapabilityDeriver;
import io.tapstate.adapters.pdk.PdkSinkPreparations;
import io.tapstate.adapters.pdk.PdkSchemaDiscoverer;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.function.BiFunction;

/** Read-only full-load inspection over the exact replacement scope and registered artifact identities. */
final class PipelineRecoveryInspector {
    record Target(String node,String source,OnFullLoad policy) {}
    record Inspection(StorageRecoveryCommands.Manifest manifest,PipelinePreparationInputs inputs,
            Map<String,SourceResource> privateContexts,Map<String,String> connectorHashes,List<Target> targets,long intentRevision,Set<String> operatorDatabases) {
        Inspection(StorageRecoveryCommands.Manifest manifest,PipelinePreparationInputs inputs,Map<String,SourceResource> privateContexts,
                Map<String,String> connectorHashes,List<Target> targets,long intentRevision) {
            this(manifest,inputs,privateContexts,connectorHashes,targets,intentRevision,Set.of());
        }
        Inspection {privateContexts=Map.copyOf(privateContexts);connectorHashes=Map.copyOf(connectorHashes);targets=List.copyOf(targets);operatorDatabases=Set.copyOf(operatorDatabases);}
        @Override public String toString(){return "PipelineRecoveryInspection[redacted]";}
    }
    private final MongoConfigurationDomain configuration;
    private final StorePort store;
    private final ConnectorProvisioner connectors;
    private final BiFunction<String,String,ConnectorCapabilities> capabilities;
    private final BiFunction<SourceResource,String,SourceModel> liveModels;
    private final Runnable active;

    PipelineRecoveryInspector(MongoConfigurationDomain configuration,StorePort store,ConnectorProvisioner connectors,Runnable active) {
        this(configuration,store,connectors,new PdkCapabilityDeriver(connectors)::derive,
                (source,hash) -> new PdkSchemaDiscoverer(id -> connectors.resolve(id,hash)).discoverStructure(new ConnectionConfig(source.id(),source.connector(),source.config())),active);
    }
    PipelineRecoveryInspector(MongoConfigurationDomain configuration,StorePort store,ConnectorProvisioner connectors,
            BiFunction<String,String,ConnectorCapabilities> capabilities,BiFunction<SourceResource,String,SourceModel> liveModels,Runnable active) {
        this.configuration=Objects.requireNonNull(configuration);this.store=Objects.requireNonNull(store);
        this.connectors=Objects.requireNonNull(connectors);this.capabilities=Objects.requireNonNull(capabilities);this.liveModels=Objects.requireNonNull(liveModels);this.active=Objects.requireNonNull(active);
    }

    Inspection inspect(CacheAuthScope auth,StorageRecoveryCommands.Selection selection) {
        return inspect(auth,selection,false,true);
    }
    Inspection inspectContinuation(CacheAuthScope auth,StorageRecoveryCommands.Selection selection) {
        return inspect(auth,selection,true,true);
    }
    Inspection inspectCurrent(CacheAuthScope auth,StorageRecoveryCommands.Selection selection) {return inspect(auth,selection,true,false);}
    private Inspection inspect(CacheAuthScope auth,StorageRecoveryCommands.Selection selection,boolean continuation,boolean probeSources) {
        return inspect(auth,selection,continuation,probeSources,null,null);
    }
    Inspection inspectCaptured(CacheAuthScope auth,StorageRecoveryCommands.Selection selection,ReadOnlyArtifactSnapshot snapshot,SourceModelSnapshot models) {
        return inspect(auth,selection,true,false,Objects.requireNonNull(snapshot),Objects.requireNonNull(models));
    }
    private Inspection inspect(CacheAuthScope auth,StorageRecoveryCommands.Selection selection,boolean continuation,boolean probeSources,
            ReadOnlyArtifactSnapshot capturedArtifacts,SourceModelSnapshot capturedModels) {
        active.run();
        if(selection.extent()!=StorageRecoveryCommands.Extent.PIPELINE || selection.pipelineIds().size()!=1)throw invalid("Confirm one Pipeline full rebuild at a time.");
        String id=selection.pipelineIds().iterator().next();
        var summary=configuration.cacheRecoveries().read(id).orElseThrow(PipelineRecoveryInspector::fenced);
        var scope=summary.targetScope();
        if(!configuration.bindings().cachePreparation().orElseThrow(PipelineRecoveryInspector::fenced).authScope().equals(auth)
                || !scope.clusterId().equals(auth.clusterId()) || scope.bindingVersion()!=auth.bindingVersion() || !scope.cacheIncarnation().equals(auth.cacheIncarnation()))throw fenced();
        var prepared=configuration.preparations().read(id).orElseThrow(PipelineRecoveryInspector::fenced);
        if(!continuation && prepared.phase()!=PipelinePreparation.Phase.UNASSIGNED || prepared.stateGeneration()!=scope.stateGeneration()
                || prepared.scope()!=null && !prepared.scope().equals(scope))throw fenced();
        var intent=configuration.intents().load(id).orElseThrow(PipelineRecoveryInspector::fenced);
        if(intent.deleted() || !intent.scope().equals(scope) || !(intent.desired().targetState()==io.tapstate.core.lifecycle.PipelineState.STOPPED
                || continuation && Set.of(io.tapstate.core.lifecycle.PipelineState.RUNNING,io.tapstate.core.lifecycle.PipelineState.PAUSED).contains(intent.desired().targetState())))throw fenced();
        var snapshot=capturedArtifacts==null?ReadOnlyArtifactSnapshot.capture(store.artifacts()):capturedArtifacts;var authored=StoredArtifacts.requirePipeline(snapshot,id);
        var pipeline=PipelineInlining.inline(authored,snapshot);
        if(pipeline.settings()!=null && pipeline.settings().readMode()==ReadMode.CDC_ONLY)
            throw invalid("Full recovery requires snapshot_and_cdc or snapshot_only read mode.");
        var sources=new LinkedHashMap<String,SourceResource>();
        pipeline.sourceIds().forEach(source -> sources.put(source,StoredArtifacts.requireSource(snapshot,source)));
        var models=capturedModels==null?SourceModelSnapshot.capture(id,sources.values(),store.schemas()):capturedModels;models.requirePipeline(id);
        var inputs=models.preparationInputs(scope,authored,pipeline,sources);
        if(inputs.selectedTables().values().stream().allMatch(Set::isEmpty))throw invalid("Full recovery requires a non-empty resolved source selection.");
        var artifacts=new TreeMap<String,String>();var modelHashes=new TreeMap<String,String>();
        var originalContexts=new LinkedHashMap<String,SourceResource>();
        var resolver=new TargetModelResolver(store,snapshot,models::model);
        for(var source:sources.values()) {
            var registered=inspectConnector(source,artifacts);requireCapability(source,registered,"batch_read_function");
            if(source.mode()==SourceMode.CDC && (pipeline.settings()==null || pipeline.settings().readMode()!=ReadMode.SNAPSHOT_ONLY))
                requireCapability(source,registered,"stream_read_function");
            var model=models.model(source);
            if(model==null)throw new TapstateException(ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED,Map.of("source",source.id()),null);
            var selected=inputs.selectedTables().get(source.id());
            for(var table:model.tables())if(selected.contains(table.name()) && table.fields().stream().anyMatch(field -> field.type()==TapstateType.UNKNOWN))
                throw new TapstateException(ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED,Map.of("source",source.id()),null);
            var resolved=resolver.resolveAll(source.id());
            for(String table:selected) {
                var target=resolved.get(table);if(target==null)throw fenced();
                modelHashes.put(source.id()+"."+table,PdkSinkPreparations.modelHash(target));
            }
            if(probeSources) {
                active.run();var live=liveModels.apply(source,artifacts.get(source.connector()));active.run();
                if(live==null || !selected.equals(Set.copyOf(PipelineTableSelection.resolve(pipeline,source,live))))throw staleModel(source);
                var actual=new TargetModelResolver(store,snapshot,ignored -> live).resolveAll(source.id());
                for(String table:selected)if(actual.get(table)==null || !PdkSinkPreparations.modelHash(actual.get(table)).equals(modelHashes.get(source.id()+"."+table)))
                    throw staleModel(source);
            }
            originalContexts.put(RequiredNoteOwner.namespace(new PipelineNode(id,source.id()),PipelineNode.Purpose.CAPTURE),source);
            boolean shared=pipeline.sources().stream().anyMatch(reference -> reference.id().equals(source.id()) && reference instanceof SourceRef.Spec spec && spec.srs())
                    && (pipeline.settings()==null || pipeline.settings().readMode()!=ReadMode.SNAPSHOT_ONLY);
            if(shared) {
                var retained=configuration.capturePreparations().inspectSharedOrigin(new CacheBusinessScope(auth.clusterId(),auth.bindingVersion(),auth.cacheIncarnation()),source.id(),CanonicalHash.of(source));
                var expected=ConnectorNoteOrigin.capture(source.connector(),artifacts.get(source.connector()),source.config());
                if(retained.isPresent() && !retained.orElseThrow().equals(expected))throw new TapstateException(IoError.NOTE_ORIGIN_MISMATCH,Map.of(),null);
            }
        }
        var targets=new ArrayList<Target>();
        if(pipeline.serve() instanceof ServeBlock.Inline serve && serve.sync()!=null)for(var sync:serve.sync()) {
            var target=StoredArtifacts.requireSource(snapshot,sync.source());var policy=sync.onFullLoad()==null?OnFullLoad.APPEND:sync.onFullLoad();
            inspectTarget(target,policy,artifacts);
            String node=PipelineConnectorNodes.syncNodeId(sync);targets.add(new Target(node,target.id(),policy));
            originalContexts.put(RequiredNoteOwner.namespace(new PipelineNode(id,node),PipelineNode.Purpose.SYNC),target);
        }
        if(pipeline.view() instanceof ViewBlock.Inline view) {
            String managed=snapshot.managedDefaultSourceId().orElseThrow(PipelineRecoveryInspector::fenced);
            var target=StoredArtifacts.requireSource(snapshot,ViewTargetResolver.resolve(view,managed).sourceId());
            var policy=view.onFullLoad()==null?OnFullLoad.APPEND:view.onFullLoad();
            inspectTarget(target,policy,artifacts);targets.add(new Target(view.id(),target.id(),policy));
            originalContexts.put(RequiredNoteOwner.namespace(new PipelineNode(id,view.id()),PipelineNode.Purpose.VIEW),target);
        }
        if(targets.isEmpty())throw invalid("Full recovery requires a configured write target.");
        for(var context:configuration.cacheRecoveries().inspectPrivateContexts(id)) {
            var desired=originalContexts.get(context.scope().owner().namespace());
            if(context.completed() || context.origin().isEmpty() || desired==null)continue;
            var origin=context.origin().orElseThrow();var expected=ConnectorNoteOrigin.capture(desired.connector(),artifacts.get(desired.connector()),desired.config());
            if(!origin.equals(expected)) {
                ConnectorNoteOrigin.requireReadable(origin);
                connectors.resolve(origin.connectorId(),origin.artifactHash());capabilities.apply(origin.connectorId(),origin.artifactHash());
                configuration.cacheRecoveries().requirePrivateRetirable(id,context.scope());
            }
        }
        var definitions=new TreeMap<String,String>();var queue=new ArrayDeque<Resource>();queue.add(authored);
        while(!queue.isEmpty()) {
            var resource=queue.remove();if(definitions.putIfAbsent(resource.id(),CanonicalHash.of(resource))!=null)continue;
            for(var edge:ReferenceGraph.declaredReferences(resource)) {
                var dependency=snapshot.get(edge.id()).orElseThrow(PipelineRecoveryInspector::fenced);
                if(!dependency.kind().equals(edge.kind()))throw fenced();queue.add(dependency);
            }
        }
        // The managed view target is implicit in the authoring grammar, but explicit in this confirmation.
        for(var target:targets)definitions.put(target.source(),CanonicalHash.of(StoredArtifacts.requireSource(snapshot,target.source())));
        var selections=new TreeMap<String,Object>();inputs.selectedTables().forEach((source,tables) -> selections.put(source,tables.stream().sorted().toList()));
        String hash=CanonicalHash.ofText(JsonWriter.write(List.of("confirmed-pipeline-full",auth.clusterId(),auth.bindingVersion(),auth.cacheIncarnation(),auth.authGeneration(),
                id,scope.stateGeneration(),summary.recoveryId(),selections,artifacts,modelHashes,
                targets.stream().map(target -> List.of(target.node(),target.source(),target.policy().name())).toList())));
        var operatorDatabases=new TreeSet<String>();
        if(pipeline.transforms()!=null)for(var step:pipeline.transforms())
            if(step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest && nest.state()!=null)
                operatorDatabases.add(nest.state().database());
        active.run();return new Inspection(new StorageRecoveryCommands.Manifest(selection,Set.of(id),definitions,hash,configuration.configurationRevision()),
                inputs,originalContexts,artifacts,targets,intent.intentRevision(),operatorDatabases);
    }
    private Set<String> inspectConnector(SourceResource source,Map<String,String> hashes) {
        var ref=connectors.resolve(source.connector());String hash=ref.contentHash();
        if(hash==null || !hash.matches("[a-f0-9]{64}"))throw new TapstateException(ConnectorError.LOAD_FAILED,Map.of("connector",source.connector()),null);
        String prior=hashes.putIfAbsent(source.connector(),hash);if(prior!=null && !prior.equals(hash))throw fenced();
        active.run();var result=capabilities.apply(source.connector(),hash).capabilityIds();active.run();return result;
    }
    private void inspectTarget(SourceResource target,OnFullLoad policy,Map<String,String> hashes) {
        var functions=inspectConnector(target,hashes);requireCapability(target,functions,"write_record_function");
        if(policy==OnFullLoad.CLEAR && !(functions.contains("clear_table_function")
                || Set.of("mongodb","mongodb-atlas").contains(target.connector()) && functions.contains("execute_command_function")))
            requireCapability(target,functions,"clear_table_function");
        if(policy==OnFullLoad.FAIL)requireCapability(target,functions,"count_by_partition_filter_function");
    }
    private static void requireCapability(SourceResource source,Set<String> functions,String capability) {
        if(!functions.contains(capability))throw new TapstateException(ConnectorError.CAPABILITY_MISSING,Map.of("connector",source.connector(),"capability",capability),null);
    }
    private static TapstateException invalid(String reason){return new TapstateException(ControlError.MALFORMED_REQUEST,Map.of("reason",reason),null);}
    private static TapstateException staleModel(SourceResource source){return new TapstateException(ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED,Map.of("source",source.id()),null);}
    private static TapstateException fenced(){return new TapstateException(ControlError.STORAGE_SCOPE_CONFLICT,Map.of(),null);}
}
