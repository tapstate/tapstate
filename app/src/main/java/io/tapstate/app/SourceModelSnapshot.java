package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.*;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One per-source discovery choice shared by preparation, deferred planning and capture. */
final class SourceModelSnapshot {
    private final String pipeline;
    private final Map<String,Optional<DiscoveredSourceModel>> models;
    private final Map<String, DiscoveredSourceModel> retainedByBinding;

    private SourceModelSnapshot(String pipeline, Map<String, Optional<DiscoveredSourceModel>> models,
            Map<String, DiscoveredSourceModel> retainedByBinding) {
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
        this.models = Map.copyOf(models);
        this.retainedByBinding = Map.copyOf(retainedByBinding);
    }
    static SourceModelSnapshot capture(String pipeline,Collection<SourceResource> sources,SchemaStore schemas) {
        var captured = java.util.List.copyOf(sources); var models = new LinkedHashMap<String,Optional<DiscoveredSourceModel>>();
        for (var source : captured) if (!models.containsKey(source.id())) models.put(source.id(),schemas.get(source.id()));
        return new SourceModelSnapshot(pipeline, models, Map.of());
    }
    SourceModel model(SourceResource source) {
        if (!models.containsKey(source.id())) throw fenced();
        return SourceDiscovery.matched(models.get(source.id()),source);
    }

    SourceModel modelForBinding(String binding, SourceResource source) {
        var retained = binding == null ? null : retainedByBinding.get(binding);
        if (retained == null) return model(source);
        if (!retained.connectionId().equals(source.id()) || !retained.connectorId().equals(source.connector())) {
            throw fenced();
        }
        return retained.model();
    }

    /** Uses exact retained observations for one input binding while leaving public discovery intact. */
    SourceModelSnapshot withRetainedTables(String binding, SourceResource source, Map<String, SourceTable> retained) {
        if (!models.containsKey(source.id())) throw fenced();
        var discovery = models.get(source.id()).orElseThrow(this::fenced);
        if (!discovery.connectorId().equals(source.connector())) throw fenced();
        var tables = new java.util.ArrayList<SourceTable>();
        var remaining=new LinkedHashMap<>(retained);
        for (var observed : discovery.model().tables()) {
            var pinned=remaining.remove(observed.name());
            tables.add(pinned==null?observed:new SourceTable(pinned.name(),pinned.fields(),pinned.primaryKey(),pinned.indexes(),observed.approximateRowCount()));
        }
        if(!remaining.isEmpty())throw fenced();
        var copy = new LinkedHashMap<>(retainedByBinding);
        copy.put(binding, new DiscoveredSourceModel(discovery.connectionId(), discovery.connectorId(),
                discovery.discoveredAt(), new SourceModel(tables)));
        return new SourceModelSnapshot(pipeline, models, copy);
    }
    PipelinePreparationInputs preparationInputs(PipelineIntentScope scope,PipelineResource original,PipelineResource selected,Map<String,SourceResource> sources) {
        requirePipeline(scope.pipelineId());
        if (!original.id().equals(scope.pipelineId()) || !selected.id().equals(original.id()) || !original.sources().equals(selected.sources())) throw fenced();
        var selections = new LinkedHashMap<String,java.util.Set<String>>();
        for (var ref : original.sources()) {
            var source = sources.get(ref.id()); if (source == null) throw fenced();
            selections.put(ref.binding(),java.util.Set.copyOf(PipelineTableSelection.resolve(selected,ref,source,model(source))));
        }
        return PipelinePreparationInputs.of(scope,original,sources,selections);
    }
    void requirePipeline(String pipeline) { if (!this.pipeline.equals(pipeline)) throw fenced(); }
    private TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null); }
}
