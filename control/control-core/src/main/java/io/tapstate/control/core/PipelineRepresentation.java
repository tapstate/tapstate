package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;

import java.util.List;
import java.util.Objects;

/** Projects a canonical Pipeline artifact and its resolved Sources into a structured view. */
public final class PipelineRepresentation {

    private final PipelineDagProjection dagProjection = new PipelineDagProjection();

    /** Builds a Pipeline view while preserving the declared Source reference order. */
    public PipelineView toView(
            PipelineResource pipeline, String contentHash, List<PipelineSourceSummary> sourceSummaries) {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(contentHash, "contentHash");
        Objects.requireNonNull(sourceSummaries, "sourceSummaries");
        if (!pipeline.sources().equals(sourceSummaries.stream().map(PipelineSourceSummary::id).toList())) {
            throw new IllegalArgumentException("source summaries must match declared pipeline source references");
        }
        return new PipelineView(
                pipeline.id(),
                pipeline.metadata(),
                sourceSummaries,
                pipeline.transforms(),
                pipeline.view(),
                pipeline.serve(),
                pipeline.settings(),
                pipeline.experimental(),
                dagProjection.project(pipeline, sourceSummaries),
                contentHash);
    }
}
