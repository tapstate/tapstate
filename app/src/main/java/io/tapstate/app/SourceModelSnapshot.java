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
    private SourceModelSnapshot(String pipeline,Map<String,Optional<DiscoveredSourceModel>> models) {
        this.pipeline = Objects.requireNonNull(pipeline,"pipeline"); this.models = Map.copyOf(models);
    }
    static SourceModelSnapshot capture(String pipeline,Collection<SourceResource> sources,SchemaStore schemas) {
        var captured = java.util.List.copyOf(sources); var models = new LinkedHashMap<String,Optional<DiscoveredSourceModel>>();
        for (var source : captured) if (!models.containsKey(source.id())) models.put(source.id(),schemas.get(source.id()));
        return new SourceModelSnapshot(pipeline,models);
    }
    SourceModel model(SourceResource source) {
        Optional<DiscoveredSourceModel> captured = models.get(source.id());
        if (captured == null) throw fenced(); return SourceDiscovery.matched(captured,source);
    }
    PipelinePreparationInputs preparationInputs(PipelineIntentScope scope,PipelineResource original,PipelineResource selected,Map<String,SourceResource> sources) {
        requirePipeline(scope.pipelineId());
        if (!original.id().equals(scope.pipelineId()) || !selected.id().equals(original.id()) || !original.sources().equals(selected.sources())) throw fenced();
        var selections = new LinkedHashMap<String,java.util.Set<String>>();
        for (String id : original.sourceIds()) {
            var source = sources.get(id); if (source == null) throw fenced();
            selections.put(id,java.util.Set.copyOf(PipelineTableSelection.resolve(selected,source,model(source))));
        }
        return PipelinePreparationInputs.of(scope,original,sources,selections);
    }
    void requirePipeline(String pipeline) { if (!this.pipeline.equals(pipeline)) throw fenced(); }
    private TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null); }
}
