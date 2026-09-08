package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Structured static Pipeline artifact returned by the control layer. */
public record PipelineView(
        String id,
        Metadata metadata,
        List<PipelineSourceSummary> sources,
        List<Map<String, Object>> transforms,
        Map<String, Object> view,
        Map<String, Object> serve,
        Map<String, Object> settings,
        Map<String, Object> experimental,
        PipelineDag dag,
        String contentHash,
        PipelineStatus status) {

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
