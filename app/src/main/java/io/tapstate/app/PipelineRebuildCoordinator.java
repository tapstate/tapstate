package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.function.*;

/** Executes one confirmed replacement through existing CONFIG and CACHE commands after writer retirement. */
final class PipelineRebuildCoordinator {
    /** Must prove the original job, writers, capture and hot placements retired, including on retry. */
    @FunctionalInterface interface RetiredExecution {
        void require(PipelineRebuildRequest original,Optional<OperatorRunManifest> manifest);
    }
    record Stores(ArtifactStore artifacts,SchemaStore schemas,PipelineStoreViews pipelines,SrsConsumerStore consumers,
                  PipelineStateRebuilds configuration,PipelineCacheRebuilds cache) {
        Stores {Objects.requireNonNull(artifacts);Objects.requireNonNull(schemas);Objects.requireNonNull(pipelines);Objects.requireNonNull(consumers);Objects.requireNonNull(configuration);Objects.requireNonNull(cache);}
    }
    private final Stores stores;
    private final PipelinePrivateNoteRetirement notes;
    private final Supplier<WorkloadClaimFence> node;
    private final Runnable requireActiveNode;
    private final RetiredExecution retired;
    private final Duration ttl;
    private final Clock clock;
    PipelineRebuildCoordinator(Stores stores,PipelinePrivateNoteRetirement notes,Supplier<WorkloadClaimFence> node,
            Runnable requireActiveNode,RetiredExecution retired,Duration ttl,Clock clock) {
        this.stores=Objects.requireNonNull(stores);this.notes=Objects.requireNonNull(notes);this.node=Objects.requireNonNull(node);
        this.requireActiveNode=Objects.requireNonNull(requireActiveNode);this.retired=Objects.requireNonNull(retired);
        this.ttl=Objects.requireNonNull(ttl);this.clock=Objects.requireNonNull(clock);
    }
    PipelinePreparation rebuild(PipelineConvergencePass pass,Optional<OperatorRunManifest> manifest) {
        requireActiveNode.run();pass.requirePipeline(pass.scope().pipelineId());
        if(!pass.intent().desired().purgeState())throw fenced();
        var closure=closure(pass.scope());var state=pass.state().read(pass.scope().pipelineId()).orElseThrow(PipelineRebuildCoordinator::fenced);
        String operation="full-"+CanonicalHash.ofText(pass.scope().toString()+":"+pass.intent().intentRevision());
        var request=new PipelineRebuildRequest(operation,pass.intent(),pass.claim(),state.epoch(),closure.inputs(),closure.dependencies());
        retired.require(request,manifest);requireActiveNode.run();
        var original=stores.configuration().begin(request);
        return execute(original,manifest);
    }
    /** Reading an existing pending operation never creates another generation or reissues first-creation authority. */
    Optional<PipelinePreparation> resume(PipelineIntentScope originalScope,Optional<OperatorRunManifest> manifest) {
        requireActiveNode.run();var closure=closure(originalScope);
        var pending=stores.configuration().pending(originalScope.pipelineId(),closure.inputs(),closure.dependencies());
        if(pending.isEmpty())return Optional.empty();
        if(!pending.orElseThrow().request().originalScope().equals(originalScope))throw fenced();
        return Optional.of(execute(pending.orElseThrow(),manifest));
    }
    private PipelinePreparation execute(PipelineRebuildPreparation original,Optional<OperatorRunManifest> manifest) {
        var request=original.request();var currentNode=Objects.requireNonNull(node.get(),"currentNode");
        Runnable guard=() -> {requireActiveNode.run();retired.require(request,manifest);requireActiveNode.run();};
        guard.run();stores.configuration().requirePending(original);
        for(var member:stores.consumers().guardedBy(guard).forPipeline(request.originalScope())) {
            guard.run();if(member.detached())continue;
            stores.consumers().guardedBy(guard).detachForRebuild(original,member,currentNode).orElseThrow(PipelineRebuildCoordinator::fenced);
        }
        var snapshot=ReadOnlyArtifactSnapshot.capture(stores.artifacts());
        var pipeline=PipelineInlining.inline(StoredArtifacts.requirePipeline(snapshot,request.originalScope().pipelineId()),snapshot);
        var desired=new HashMap<String,SourceResource>();
        for(var ref:pipeline.sources())desired.put(RequiredNoteOwner.namespace(new PipelineNode(pipeline.id(),ref.id()),PipelineNode.Purpose.CAPTURE),StoredArtifacts.requireSource(snapshot,ref.id()));
        if(pipeline.serve() instanceof ServeBlock.Inline serve && serve.sync()!=null)for(var sync:serve.sync())desired.put(
                RequiredNoteOwner.namespace(new PipelineNode(pipeline.id(),PipelineConnectorNodes.syncNodeId(sync)),PipelineNode.Purpose.SYNC),StoredArtifacts.requireSource(snapshot,sync.source()));
        // View identity is retained here; its deployment-owned target is reconciled by the binding operation.
        notes.reconcile(original,Map.copyOf(desired),guard);guard.run();stores.configuration().requirePending(original);
        var receipt=stores.cache().prepare(original,currentNode,ttl,clock.instant());guard.run();
        while(stores.cache().reclaim(receipt,currentNode)>0)guard.run();
        guard.run();return stores.configuration().complete(receipt);
    }
    private record Closure(PipelinePreparationInputs inputs,Map<String,String> dependencies) {}
    private Closure closure(PipelineIntentScope original) {
        var snapshot=ReadOnlyArtifactSnapshot.capture(stores.artifacts());var authored=StoredArtifacts.requirePipeline(snapshot,original.pipelineId());
        var expanded=PipelineInlining.inline(authored,snapshot);var definitions=new LinkedHashMap<String,SourceResource>();
        expanded.sourceIds().forEach(id -> definitions.put(id,StoredArtifacts.requireSource(snapshot,id)));
        var successor=new PipelineIntentScope(original.clusterId(),original.bindingVersion(),original.cacheIncarnation(),original.pipelineId(),Math.addExact(original.stateGeneration(),1));
        var models=SourceModelSnapshot.capture(original.pipelineId(),definitions.values(),stores.schemas());
        var inputs=models.preparationInputs(successor,authored,expanded,definitions);
        var dependencies=new HashMap<String,String>();var queue=new ArrayDeque<Resource>();var visited=new HashSet<String>();queue.add(authored);
        while(!queue.isEmpty()) {
            var resource=queue.remove();if(!visited.add(resource.id()))continue;
            for(var edge:ReferenceGraph.declaredReferences(resource)) {
                var required=snapshot.get(edge.id()).orElseThrow(PipelineRebuildCoordinator::fenced);
                if(!edge.kind().equals(required.kind()))throw fenced();queue.add(required);
                if(!inputs.sourceHashes().containsKey(required.id()))dependencies.put(required.id(),CanonicalHash.of(required));
            }
        }
        requireActiveNode.run();return new Closure(inputs,Map.copyOf(dependencies));
    }
    private static TapstateException fenced(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
