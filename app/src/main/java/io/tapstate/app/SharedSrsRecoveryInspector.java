package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.LivePipelines;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.spi.store.*;
import java.util.*;

/** Actual retained consumer closure, with CONFIG-only dependency checks kept separate from CACHE inspection. */
final class SharedSrsRecoveryInspector {
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final CacheBusinessScope business;
    private final Runnable active;

    SharedSrsRecoveryInspector(MongoConfigurationDomain configuration,MongoCacheDomain cache,CacheBusinessScope business,Runnable active) {
        this.configuration=Objects.requireNonNull(configuration);this.cache=Objects.requireNonNull(cache);
        this.business=Objects.requireNonNull(business);this.active=Objects.requireNonNull(active);
    }

    StorageRecoveryCommands.Manifest inspect(StorageRecoveryCommands.Selection selection) {
        active.run();long revision=configuration.configurationRevision();
        if(selection.extent()!=StorageRecoveryCommands.Extent.SHARED_SRS)throw fenced();
        String capture=SourceCaptureIdentity.sharedCapture(selection.canonicalChain());
        var previous=configuration.capturePreparations().read(business,capture).orElseThrow(SharedSrsRecoveryInspector::fenced);
        requireSelection(selection,previous);
        var observed=snapshot(previous);
        var manifest=manifest(selection,observed,revision);
        requireRest(observed);requireConfiguration(manifest);
        if(revision!=configuration.configurationRevision())throw fenced();return manifest;
    }

    SharedSrsRecoverySnapshot snapshot(CapturePreparation previous) {
        active.run();return cache.srs(business,active).consumers().inspectSharedRecovery(previous);
    }

    StorageRecoveryCommands.Manifest manifest(StorageRecoveryCommands.Selection selection,SharedSrsRecoverySnapshot observed,long revision) {
        active.run();requireSelection(selection,observed.previous());
        var artifacts=ReadOnlyArtifactSnapshot.capture(configuration.artifacts());
        var definitions=definitions(artifacts,observed);
        var token=configuration.requiredNotes().guardedBy(active).open(new RequiredNoteOwner(business.bindingVersion(),
                "pdk.chain."+selection.canonicalChain(),RequiredNoteOwner.Kind.CHAIN,selection.canonicalChain(),0));
        var origin=configuration.requiredNotes().guardedBy(active).origin(token);ConnectorNoteOrigin.requireReadable(origin);
        for(String sourceId:observed.consumers().stream().map(consumer -> consumer.context().key().sourceId()).collect(java.util.stream.Collectors.toSet())) {
            var source=StoredArtifacts.requireSource(artifacts,sourceId);
            String canonical=SourceCaptureIdentity.resolve(source.connector(),source.config(),source.srs()==null?null:source.srs().key());
            if(!canonical.equals(selection.canonicalChain()) || !SourceCaptureIdentity.physical(source.connector(),source.config()).equals(observed.previous().sourceIdentityHash())
                    || !origin.equals(ConnectorNoteOrigin.capture(source.connector(),origin.artifactHash(),source.config())))throw fenced();
        }
        for(var resource:artifacts.list())if(resource instanceof SourceResource source) {
            String canonical=SourceCaptureIdentity.resolve(source.connector(),source.config(),source.srs()==null?null:source.srs().key());
            if(canonical.equals(selection.canonicalChain()) && (!SourceCaptureIdentity.physical(source.connector(),source.config())
                    .equals(observed.previous().sourceIdentityHash()) || !origin.equals(ConnectorNoteOrigin.capture(source.connector(),origin.artifactHash(),source.config()))))throw fenced();
        }
        String dependency=CanonicalHash.ofText(JsonWriter.write(List.of("shared-srs-recovery",observed.controlHash(),definitions,
                token.generation(),origin.connectorId(),origin.artifactHash(),CanonicalHash.ofText(Base64.getEncoder().encodeToString(origin.settings())))));
        return new StorageRecoveryCommands.Manifest(selection,observed.pipelines().keySet(),definitions,dependency,revision,observed);
    }

    /** No SRS/CACHE business reads, connector calls or member RPC: usable inside the original CONFIG guard. */
    void requireConfiguration(StorageRecoveryCommands.Manifest original) {
        var actual=manifest(original.selection(),original.sharedSrs(),original.configurationRevision());
        if(!original.definitionHashes().equals(actual.definitionHashes()) || !original.dependencyHash().equals(actual.dependencyHash()))throw fenced();
        for(var pipeline:original.sharedSrs().pipelines().values()) {
            String id=pipeline.scope().pipelineId();
            var mapping=configuration.pipelineStates().read(id).orElseThrow(SharedSrsRecoveryInspector::fenced);
            var preparation=configuration.preparations().read(id).orElseThrow(SharedSrsRecoveryInspector::fenced);
            var intent=configuration.intents().load(id).orElseThrow(SharedSrsRecoveryInspector::fenced);
            if(mapping.phase()!=PipelineStateMapping.Phase.ACTIVE || mapping.stateGeneration()!=pipeline.scope().stateGeneration()
                    || preparation.phase()!=PipelinePreparation.Phase.COMPLETE || !pipeline.scope().equals(preparation.scope())
                    || intent.deleted() || !intent.scope().equals(pipeline.scope()))throw fenced();
        }
    }

    void requireRest(SharedSrsRecoverySnapshot observed) {
        active.run();
        for(var pipeline:observed.pipelines().values()) {
            var scope=pipeline.scope();String id=scope.pipelineId();
            var actual=cache.pipelineStates(scope).read(id).orElseThrow(SharedSrsRecoveryInspector::fenced);
            var intent=configuration.intents().load(id).orElseThrow(SharedSrsRecoveryInspector::fenced);
            if(intent.deleted() || !intent.scope().equals(scope))throw fenced();
            if(!LivePipelines.isAtRest(actual,intent.desired()))throw new TapstateException(ArtifactError.PIPELINE_NOT_STOPPED,
                    Map.of("id",id,"actual",StateJson.parse(actual.stateJson()).name(),"desired",intent.desired().targetState().name()),null);
        }
    }

    private Map<String,String> definitions(ReadOnlyArtifactSnapshot artifacts,SharedSrsRecoverySnapshot observed) {
        var hashes=new TreeMap<String,String>();var queue=new ArrayDeque<Resource>();
        for(String id:observed.pipelines().keySet())queue.add(StoredArtifacts.requirePipeline(artifacts,id));
        while(!queue.isEmpty()) {
            var resource=queue.remove();if(hashes.putIfAbsent(resource.id(),CanonicalHash.of(resource))!=null)continue;
            for(var edge:ReferenceGraph.declaredReferences(resource)) {
                var dependency=artifacts.get(edge.id()).orElseThrow(SharedSrsRecoveryInspector::fenced);
                if(!dependency.kind().equals(edge.kind()))throw fenced();queue.add(dependency);
            }
            if(resource instanceof PipelineResource authored) {
                var pipeline=PipelineInlining.inline(authored,artifacts);
                if(pipeline.view() instanceof ViewBlock.Inline view) {
                    String managed=artifacts.managedDefaultSourceId().orElseThrow(SharedSrsRecoveryInspector::fenced);
                    queue.add(StoredArtifacts.requireSource(artifacts,ViewTargetResolver.resolve(view,managed).sourceId()));
                }
            }
        }
        return hashes;
    }
    private void requireSelection(StorageRecoveryCommands.Selection selected,CapturePreparation previous) {
        if(selected.extent()!=StorageRecoveryCommands.Extent.SHARED_SRS || previous.mode()!=CapturePreparation.Mode.SHARED_SRS
                || !business.equals(previous.scope()) || !selected.canonicalChain().equals(previous.canonicalChainId())
                || !selected.physicalChain().equals(previous.chainId())
                || !previous.captureId().equals(SourceCaptureIdentity.sharedCapture(previous.canonicalChainId())))throw fenced();
    }
    private static TapstateException fenced(){return ThreeLayerStorePort.refused();}
}
