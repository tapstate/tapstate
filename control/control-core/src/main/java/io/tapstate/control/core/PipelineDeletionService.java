package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Coordinates a Web Pipeline deletion across its applied artifact and authoring draft. */
public final class PipelineDeletionService {

    private final ArtifactQueryService artifacts;
    private final ArtifactMutationService mutations;
    private final PipelineDraftService drafts;

    public PipelineDeletionService(
            ArtifactQueryService artifacts,
            ArtifactMutationService mutations,
            PipelineDraftService drafts) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.mutations = Objects.requireNonNull(mutations, "mutations");
        this.drafts = Objects.requireNonNull(drafts, "drafts");
    }

    /**
     * Removes the applied Pipeline and the Draft revision the caller read. A null expected revision means
     * the caller read no Draft. Artifact guards run before any Draft is discarded. A racing Draft write is
     * reported as an incomplete removal so the remaining authoring document is never hidden.
     */
    public void delete(String principal, String pipelineId, String expectedArtifactHash,
            Long expectedDraftRevision) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(pipelineId, "pipelineId");

        artifacts.getResource(pipelineId)
                .map(StoredResource::resource)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast)
                .orElseThrow(() -> new TapstateException(
                        PipelineError.NOT_FOUND, Map.of("id", pipelineId), null));

        PipelineDraft currentDraft = drafts.find(pipelineId).orElse(null);
        if ((currentDraft == null) != (expectedDraftRevision == null)
                || (currentDraft != null && currentDraft.revision() != expectedDraftRevision)) {
            throw new TapstateException(
                    PipelineDraftError.REVISION_CONFLICT, Map.of("id", pipelineId), null);
        }

        TapstateException artifactCleanupFailure = null;
        try {
            mutations.deletePipeline(principal, pipelineId, expectedArtifactHash);
        } catch (TapstateException failure) {
            if (!ArtifactError.RECLAIM_INCOMPLETE.code().equals(failure.code().code())) {
                throw failure;
            }
            artifactCleanupFailure = failure;
        }

        if (artifactCleanupFailure != null && "pipeline-live".equals(artifactCleanupFailure.args().get("reason"))) {
            try {
                if (drafts.find(pipelineId).isEmpty()) {
                    throw artifactCleanupFailure;
                }
            } catch (TapstateException unreadable) {
                if (unreadable == artifactCleanupFailure) throw unreadable;
                artifactCleanupFailure.addSuppressed(unreadable);
            }
            throw withDraftResidue(pipelineId, artifactCleanupFailure);
        }

        TapstateException draftCleanupFailure = discardDraft(
                principal, pipelineId, expectedDraftRevision, currentDraft != null);
        if (artifactCleanupFailure != null) {
            if (draftCleanupFailure == null) {
                throw artifactCleanupFailure;
            }
            artifactCleanupFailure.addSuppressed(draftCleanupFailure);
            throw withDraftResidue(pipelineId, artifactCleanupFailure);
        }
        if (draftCleanupFailure != null) {
            throw withDraftResidue(pipelineId, draftCleanupFailure);
        }
    }

    private TapstateException discardDraft(
            String principal, String pipelineId, Long expectedRevision, boolean draftWasPresent) {
        try {
            if (draftWasPresent) {
                PipelineDraftMutation mutation = drafts.discard(principal, pipelineId, expectedRevision);
                if (mutation != PipelineDraftMutation.DELETED && mutation != PipelineDraftMutation.NOT_FOUND
                        && mutation != PipelineDraftMutation.REVISION_CONFLICT) {
                    throw new IllegalStateException("unexpected Pipeline draft deletion outcome: " + mutation);
                }
            }
            return drafts.find(pipelineId).isPresent()
                    ? new TapstateException(PipelineDraftError.REVISION_CONFLICT, Map.of("id", pipelineId), null)
                    : null;
        } catch (TapstateException failure) {
            try {
                return drafts.find(pipelineId).isPresent() ? failure : null;
            } catch (TapstateException unreadable) {
                failure.addSuppressed(unreadable);
                return failure;
            }
        }
    }

    private static TapstateException withDraftResidue(String pipelineId, TapstateException failure) {
        List<String> residue = new ArrayList<>();
        Object existingResidue = failure.args().get("residue");
        if (existingResidue instanceof List<?> values) {
            values.stream().filter(String.class::isInstance).map(String.class::cast).forEach(residue::add);
        }
        residue.add("pipeline-draft:" + pipelineId);
        return new TapstateException(ArtifactError.RECLAIM_INCOMPLETE,
                Map.of("id", pipelineId, "reason", "pipeline-delete", "residue", List.copyOf(residue)), failure);
    }
}
