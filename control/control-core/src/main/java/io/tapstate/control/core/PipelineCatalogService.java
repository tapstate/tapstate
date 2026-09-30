package io.tapstate.control.core;

import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftSummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Builds the single list projection across saved Pipeline definitions, applied artifacts, and runtime state. */
public final class PipelineCatalogService {

    private static final Logger LOG = Logger.getLogger(PipelineCatalogService.class.getName());

    private final ArtifactQueryService artifacts;
    private final PipelineDraftService drafts;
    private final DesiredStore desired;
    private final PipelineObservationQueryService observations;

    public PipelineCatalogService(
            ArtifactQueryService artifacts,
            PipelineDraftService drafts,
            DesiredStore desired,
            PipelineObservationQueryService observations) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.drafts = Objects.requireNonNull(drafts, "drafts");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.observations = Objects.requireNonNull(observations, "observations");
    }

    /** Returns one stable row for every id present in either the authoring store or artifact store. */
    public List<PipelineCatalogItem> list() {
        return list(0, Integer.MAX_VALUE);
    }

    /** Returns only the requested catalog window; per-pipeline status reads happen after this slice. */
    public List<PipelineCatalogItem> list(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("pipeline catalog offset must be non-negative and limit positive");
        }
        Map<String, ArtifactSummary> artifactById = new HashMap<>();
        for (StoredResource stored : artifacts.listResources()) {
            Resource resource = stored.resource();
            if (resource instanceof PipelineResource pipeline) {
                artifactById.put(pipeline.id(), new ArtifactSummary(pipeline, stored.contentHash()));
            }
        }

        Map<String, PipelineDraftSummary> draftById = new HashMap<>();
        for (PipelineDraftSummary draft : drafts.listSummaries()) {
            draftById.put(draft.pipelineId(), draft);
        }

        TreeSet<String> ids = new TreeSet<>(artifactById.keySet());
        ids.addAll(draftById.keySet());
        if (offset >= ids.size()) return List.of();
        int end = (int) Math.min((long) ids.size(), (long) offset + limit);
        List<String> pageIds = new ArrayList<>(ids).subList(offset, end);
        List<PipelineCatalogItem> items = new ArrayList<>(pageIds.size());
        for (String id : pageIds) {
            items.add(toItem(id, draftById.get(id), artifactById.get(id)));
        }
        return List.copyOf(items);
    }

    private PipelineCatalogItem toItem(String id, PipelineDraftSummary draft, ArtifactSummary artifact) {
        PipelineStatus observation = observations.findStatus(id).orElse(null);
        PipelineState observed = observation == null ? null : observation.state();
        PipelineState target;
        try {
            target = desired.read(id).map(state -> state.targetState()).orElse(null);
        } catch (TapstateException unreadable) {
            if (unreadable.code() != IoError.DOCUMENT_UNREADABLE) {
                throw unreadable;
            }
            LOG.log(Level.WARNING, "Ignoring unreadable desired state for Pipeline {0}", id);
            target = null;
        }
        boolean hasArtifact = artifact != null;

        String description = draft != null ? draft.description() : descriptionOf(artifact);
        String mode = draft == null ? null : draft.mode().name().toLowerCase(java.util.Locale.ROOT);
        String name = draft != null && draft.name() != null && !draft.name().isBlank() ? draft.name() : id;

        return new PipelineCatalogItem(
                id,
                name,
                description,
                mode,
                draft == null ? null : draft.revision(),
                draft == null ? null : draft.updatedAt(),
                artifact == null ? null : artifact.contentHash(),
                hasArtifact,
                new PipelineCatalogItem.Status(
                        displayState(target, observed, hasArtifact),
                        target,
                        observed,
                        observation == null ? null : observation.failure(),
                        observation == null ? null : observation.observedAt()));
    }

    private static String descriptionOf(ArtifactSummary artifact) {
        if (artifact == null) {
            return null;
        }
        Metadata metadata = artifact.pipeline().metadata();
        return metadata == null ? null : metadata.description();
    }

    private record ArtifactSummary(PipelineResource pipeline, String contentHash) {
    }

    private static PipelineCatalogItem.DisplayState displayState(
            PipelineState desired, PipelineState observed, boolean hasArtifact) {
        if (desired == PipelineState.STOPPED && observed != PipelineState.STOPPED) {
            return PipelineCatalogItem.DisplayState.STOPPING;
        }
        if (observed == PipelineState.FAILED) {
            return PipelineCatalogItem.DisplayState.FAILED;
        }
        if (observed == PipelineState.COMPLETED) {
            return PipelineCatalogItem.DisplayState.COMPLETED;
        }
        if (desired == PipelineState.RUNNING && observed != PipelineState.RUNNING) {
            return observed == PipelineState.PAUSED
                    ? PipelineCatalogItem.DisplayState.RESUMING
                    : PipelineCatalogItem.DisplayState.STARTING;
        }
        if (desired == PipelineState.PAUSED && observed != PipelineState.PAUSED) {
            return PipelineCatalogItem.DisplayState.PAUSING;
        }
        if (observed != null) {
            return PipelineCatalogItem.DisplayState.valueOf(observed.name());
        }
        if (desired != null) {
            return switch (desired) {
                case RUNNING -> PipelineCatalogItem.DisplayState.STARTING;
                case PAUSED -> PipelineCatalogItem.DisplayState.PAUSING;
                case STOPPED -> PipelineCatalogItem.DisplayState.STOPPING;
                case FAILED -> PipelineCatalogItem.DisplayState.FAILED;
                case NEW -> PipelineCatalogItem.DisplayState.NEW;
                case COMPLETED -> PipelineCatalogItem.DisplayState.COMPLETED;
            };
        }
        // An applied Pipeline with no desired intent or observation has never been started yet.
        // Keep it actionable as NEW; UNAVAILABLE is not a substitute for a missing observation.
        return PipelineCatalogItem.DisplayState.NEW;
    }
}
