package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/** Persistence contract for server-owned Pipeline authoring documents. */
public interface PipelineDraftStore {

    Optional<PipelineDraft> get(String pipelineId);

    List<PipelineDraft> list();

    PipelineDraftMutation create(PipelineDraft draft);

    PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement);

    /** Atomically writes the candidate Artifact and advances the draft publication markers. */
    PipelineDraftMutation publish(PipelineDraft.Publication publication);
}
