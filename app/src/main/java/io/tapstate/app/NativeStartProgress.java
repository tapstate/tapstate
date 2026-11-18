package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SrsConsumerStore;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsStores;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/** One start prediction captures each physical chain once for its own source nodes. */
final class NativeStartProgress {
    private record Selection(String source, boolean shared) {}
    private record Read(boolean included, Optional<SrsConsumerStore.PlanningProgress> progress) {}
    private final ThreeLayerStorePort store;
    private final PipelineResource pipeline;
    private final PipelineIntentScope scope;
    private final ArtifactStore artifacts;
    private final Function<SourceResource, SourceModel> models;
    private final Supplier<SrsStores> stores;
    private Map<String,Read> reads;

    NativeStartProgress(ThreeLayerStorePort store, PipelineResource pipeline, PipelineIntentScope scope,
            ArtifactStore artifacts, Function<SourceResource,SourceModel> models, Supplier<SrsStores> stores) {
        this.store=store; this.pipeline=pipeline; this.scope=scope;
        this.artifacts=artifacts; this.models=models; this.stores=stores;
    }

    List<String> chains() {
        load();
        return reads.entrySet().stream().filter(entry -> entry.getValue().included()).map(Map.Entry::getKey).toList();
    }

    Optional<SrsMeta> read(String chain) {
        load();
        var selected=reads.get(chain);
        if (selected==null || !selected.included()) return Optional.empty();
        if (selected.progress().isEmpty()) return store.unsharedStartProgress(chain);
        var held=selected.progress().orElseThrow(); var state=held.chain();
        return Optional.of(new SrsMeta(chain, state.resumeOverride()!=null ? new ChainPosition(null,state.resumeOverride().token())
                : state.checkpoint()==null ? null : state.checkpoint().position(),
                held.consumers().stream().map(value -> value.cursor()).toList(),List.of(),state.retention(),state.epoch(),
                state.resumeOverride()!=null ? state.resumeOverride().editedAt() : state.checkpointAt(),
                state.resumeOverride()==null && state.checkpoint()!=null));
    }

    private void load() {
        if (reads!=null) return;
        var groups=new LinkedHashMap<String,List<Selection>>();
        for (String id:pipeline.sourceIds()) {
            var source=StoredArtifacts.requireSource(artifacts,id);
            SourceCaptureResolution.forPipeline(pipeline,source,models.apply(source))
                    .flatMap(canonical -> store.startPlanningDescriptor(pipeline,source,canonical))
                    .ifPresent(resolved -> groups.computeIfAbsent(resolved.chainId().value(),ignored -> new ArrayList<>())
                            .add(new Selection(id,store.sharedStartSource(pipeline,source))));
        }
        var loaded=new LinkedHashMap<String,Read>();
        for (var entry:groups.entrySet()) {
            String chain=entry.getKey(); var selections=entry.getValue();
            var sourceIds=selections.stream().map(Selection::source).collect(java.util.stream.Collectors.toSet());
            store.planningScope(scope).verify().run();
            var progress=stores.get().consumers().planningProgress(scope,chain,sourceIds);
            store.planningScope(scope).verify().run();
            if (progress.isEmpty() && selections.stream().anyMatch(Selection::shared)) throw unavailable(chain);
            var attached=new HashSet<String>();
            if (progress.isPresent()) {
                if (!chain.equals(progress.orElseThrow().chain().miningChainId())) throw unavailable(chain);
                for (var own:progress.orElseThrow().consumers()) {
                    var context=own.context();
                    if (!context.key().miningChainId().equals(chain) || !context.key().pipelineId().equals(scope.pipelineId())
                            || context.stateGeneration()!=scope.stateGeneration() || !sourceIds.contains(context.key().sourceId())
                            || !attached.add(context.key().sourceId())) throw unavailable(chain);
                }
            }
            boolean included=selections.stream().anyMatch(selection -> !selection.shared() || attached.contains(selection.source()));
            loaded.put(chain,new Read(included,progress));
        }
        reads=Collections.unmodifiableMap(loaded);
    }

    private static TapstateException unavailable(String chain) {
        return new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",chain),null);
    }
}
