package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Metadata-only Pipeline draft projection for bounded catalogs; the graph or Wizard body stays per-id. */
public record PipelineDraftSummary(
        String pipelineId,
        PipelineDraft.Mode mode,
        String name,
        String description,
        long revision,
        String baseArtifactHash,
        Long publishedDraftRevision,
        String publishedArtifactHash,
        Instant createdAt,
        Instant updatedAt,
        String updatedBy) {

    public PipelineDraftSummary {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }
}
