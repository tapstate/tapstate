package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/** Persistence contract for server-owned Pipeline authoring documents. */
public interface PipelineDraftStore {

    Optional<PipelineDraft> get(String pipelineId);

    List<PipelineDraft> list();

    /** Metadata-only catalog read; implementations should avoid loading graph and Wizard payloads. */
    default List<PipelineDraftSummary> listSummaries() {
        return list().stream().map(PipelineDraftStore::summary).toList();
    }

    default List<PipelineDraftSummary> listSummaries(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("draft page offset must be non-negative and limit must be positive");
        }
        List<PipelineDraftSummary> items = listSummaries();
        if (offset >= items.size()) return List.of();
        int end = (int) Math.min((long) items.size(), (long) offset + limit);
        return List.copyOf(items.subList(offset, end));
    }

    /** Reads one stable page of drafts without requiring stores to materialize every authoring document. */
    default List<PipelineDraft> list(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("draft page offset must be non-negative and limit must be positive");
        }
        List<PipelineDraft> items = list();
        if (offset >= items.size()) return List.of();
        int end = (int) Math.min((long) items.size(), (long) offset + limit);
        return List.copyOf(items.subList(offset, end));
    }

    PipelineDraftMutation create(PipelineDraft draft);

    PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement);

    /** Changes the publication base only when the stored draft still has the base that was read. */
    default PipelineDraftMutation rebase(String pipelineId, long expectedRevision,
            String expectedBaseArtifactHash, PipelineDraft replacement) {
        throw new UnsupportedOperationException("draft store does not support rebase");
    }

    PipelineDraftMutation delete(String pipelineId, long expectedRevision);

    /** Atomically writes the candidate Artifact and advances the draft publication markers. */
    PipelineDraftMutation publish(PipelineDraft.Publication publication);

    private static PipelineDraftSummary summary(PipelineDraft draft) {
        return new PipelineDraftSummary(draft.pipelineId(), draft.mode(), draft.name(), draft.description(),
                draft.revision(), draft.baseArtifactHash(), draft.publishedDraftRevision(),
                draft.publishedArtifactHash(), draft.createdAt(), draft.updatedAt(), draft.updatedBy());
    }
}
