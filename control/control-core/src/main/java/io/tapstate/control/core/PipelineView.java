package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.ViewBlock;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Structured static Pipeline artifact returned by the control layer. */
public record PipelineView(
        String id,
        Metadata metadata,
        List<PipelineSourceSummary> sources,
        List<Step> transforms,
        ViewBlock view,
        ServeBlock serve,
        Settings settings,
        Map<String, Object> experimental,
        PipelineDag dag,
        String contentHash) {

    public PipelineView {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(dag, "dag");
        Objects.requireNonNull(contentHash, "contentHash");
        sources = List.copyOf(sources);
        transforms = transforms == null ? null : List.copyOf(transforms);
        experimental = SourceDraft.copyJsonMap(experimental, true);
    }
}
