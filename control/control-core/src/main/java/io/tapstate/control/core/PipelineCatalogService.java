package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftSummary;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.StorePort.PipelineCatalogState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    private final java.util.function.Predicate<String> recoveryRequired;
    private final java.util.function.Function<List<String>, Map<String, PipelineCatalogState>> batch;

    public PipelineCatalogService(
            ArtifactQueryService artifacts,
            PipelineDraftService drafts,
            DesiredStore desired,
            PipelineObservationQueryService observations) {
        this(artifacts,drafts,desired,observations,ignored -> false);
    }
    public PipelineCatalogService(ArtifactQueryService artifacts,PipelineDraftService drafts,DesiredStore desired,
            PipelineObservationQueryService observations,java.util.function.Predicate<String> recoveryRequired) {
        this(artifacts, drafts, desired, observations, recoveryRequired, ids -> Map.of());
    }

    public PipelineCatalogService(ArtifactQueryService artifacts, PipelineDraftService drafts,
            PipelineObservationQueryService observations, StorePort store) {
        this(artifacts, drafts, store.desired(), observations, store::pipelineRecoveryRequired, store::pipelineCatalogStates);
    }

    private PipelineCatalogService(ArtifactQueryService artifacts, PipelineDraftService drafts, DesiredStore desired,
            PipelineObservationQueryService observations, java.util.function.Predicate<String> recoveryRequired,
            java.util.function.Function<List<String>, Map<String, PipelineCatalogState>> batch) {
        this.batch = batch;
        this.recoveryRequired=Objects.requireNonNull(recoveryRequired);
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.drafts = Objects.requireNonNull(drafts, "drafts");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.observations = Objects.requireNonNull(observations, "observations");
    }

    /** Returns one stable row for every id present in either the authoring store or artifact store. */
    public List<PipelineCatalogItem> list() {
        return list(0, Integer.MAX_VALUE);
    }

    /** Reads the same merged projection as the catalog for one Pipeline id. */
    public Optional<PipelineCatalogItem> find(String id) {
        Objects.requireNonNull(id, "id");
        ArtifactSummary artifact = artifacts.getResource("pipeline", id)
                .filter(stored -> stored.resource() instanceof PipelineResource)
                .map(stored -> new ArtifactSummary((PipelineResource) stored.resource(), stored.contentHash()))
                .orElse(null);
        PipelineDraftSummary draft = drafts.find(id)
                .map(value -> new PipelineDraftSummary(value.pipelineId(), value.mode(), value.name(),
                        value.description(), value.revision(), value.baseArtifactHash(),
                        value.publishedDraftRevision(), value.publishedArtifactHash(), value.createdAt(),
                        value.updatedAt(), value.updatedBy()))
                .orElse(null);
        if (artifact == null && draft == null) return Optional.empty();
        var states = artifact == null ? Map.<String, PipelineCatalogState>of() : batch.apply(List.of(id));
        return Optional.of(toItem(id, draft, artifact, states.get(id), currentEngineStates(states).get(id)));
    }

    /** Returns only the requested catalog window; per-pipeline status reads happen after this slice. */
    public List<PipelineCatalogItem> list(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("pipeline catalog offset must be non-negative and limit positive");
        }
        Map<String, ArtifactSummary> artifactById = new HashMap<>();
        for (StoredResource stored : artifacts.listResources("pipeline")) {
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
        var states = batch.apply(pageIds.stream().filter(artifactById::containsKey).toList());
        var engineStates = currentEngineStates(states);
        List<PipelineCatalogItem> items = new ArrayList<>(pageIds.size());
        for (String id : pageIds) {
            items.add(toItem(id, draftById.get(id), artifactById.get(id), states.get(id), engineStates.get(id)));
        }
        return List.copyOf(items);
    }

    /** Checks only authoring and artifact stores; status-watch admission must not read desired intent. */
    public boolean containsDefinition(String id) {
        Objects.requireNonNull(id, "id");
        boolean hasArtifact = artifacts.getResource(id)
                .filter(stored -> stored.resource() instanceof PipelineResource)
                .isPresent();
        return hasArtifact || drafts.find(id).isPresent();
    }

    private Map<String, PipelineState> currentEngineStates(Map<String, PipelineCatalogState> states) {
        var scopes = new java.util.LinkedHashMap<String, io.tapstate.spi.store.PipelineIntentScope>();
        states.forEach((id, state) -> {
            if (state.observation().isEmpty() && state.scope() != null
                    && (state.checkpoint() == PipelineState.RUNNING || state.checkpoint() == PipelineState.PAUSED))
                scopes.put(id, state.scope());
        });
        return scopes.isEmpty() ? Map.of() : observations.currentEngineStates(scopes);
    }

    private PipelineCatalogItem toItem(String id, PipelineDraftSummary draft, ArtifactSummary artifact, PipelineCatalogState stateFacts,
            PipelineState engineState) {
        boolean hasArtifact = artifact != null;
        var observation = readObservation(id, hasArtifact, stateFacts);
        var status = statusOf(id, hasArtifact, stateFacts, engineState,
                observation == null ? null : observations.statusOf(observation));
        String description = draft != null ? draft.description() : descriptionOf(artifact);
        String mode = draft == null ? null : draft.mode().name().toLowerCase(java.util.Locale.ROOT);
        String name = draft != null && draft.name() != null && !draft.name().isBlank() ? draft.name() : id;
        var syncLag = status.displayState() == PipelineCatalogItem.UiState.RUNNING
                && status.state() == PipelineCatalogItem.DisplayState.RUNNING
                ? observations.syncLagOf(observation)
                : null;
        return new PipelineCatalogItem(id, name, description, mode, draft == null ? null : draft.revision(),
                draft == null ? null : draft.updatedAt(), artifact == null ? null : artifact.contentHash(),
                hasArtifact, status, syncLag);
    }

    private PipelineCatalogItem.Status statusOf(String id, boolean hasArtifact, PipelineCatalogState stateFacts,
            PipelineState engineState, PipelineStatus observation) {
        boolean needsRecovery = stateFacts == null && hasArtifact && recoveryRequired.test(id);
        PipelineState observed = observation == null ? null : observation.observedState();
        PipelineState checkpoint = stateFacts == null ? null : stateFacts.checkpoint();
        PipelineState effective = effectiveState(engineState, observation, stateFacts, checkpoint);
        var intent = readIntent(id, hasArtifact, stateFacts);
        PipelineState target = intent.desired() == null ? null : intent.desired().targetState();
        // Deferred preparation can be stopped before it creates any runtime state to converge.
        boolean stoppedBeforeFirstRun = target == PipelineState.STOPPED && !intent.desired().purgeState()
                && stateFacts != null && stateFacts.scope() == null && checkpoint == PipelineState.NEW
                && observation == null;
        boolean recoveryAtRest = needsRecovery
                && (observed == null || observed == PipelineState.NEW || observed == PipelineState.STOPPED);
        PipelineCatalogItem.DisplayState state = recoveryAtRest ? asDisplayState(observed)
                : displayState(target, effective, intent.failure() == null);
        if (stoppedBeforeFirstRun) {
            state = PipelineCatalogItem.DisplayState.NEW;
        }

        return new PipelineCatalogItem.Status(
                runtimeState(engineState, observation, stateFacts, checkpoint, effective, state),
                recoveryAtRest ? PipelineCatalogItem.UiState.RECOVERY_REQUIRED
                        : PipelineCatalogItem.UiState.of(state, hasArtifact),
                target, observed, observation == null ? null : observation.failure(),
                observation == null ? null : observation.observedAt(),
                intent.failure(), stopState(intent.desired(), stateFacts, state, recoveryAtRest));
    }

    private Observation readObservation(String id, boolean hasArtifact, PipelineCatalogState stateFacts) {
        if (stateFacts != null) {
            return stateFacts.observation().orElse(null);
        }
        return hasArtifact ? observations.findObservation(id).orElse(null) : null;
    }

    private static PipelineState effectiveState(PipelineState engineState, PipelineStatus observation,
            PipelineCatalogState stateFacts, PipelineState checkpoint) {
        if (engineState != null) return engineState;
        PipelineState effective = observation == null ? null : observation.state();
        if (effective == null && stateFacts != null && stateFacts.runningObservationUnavailable()
                && checkpoint == PipelineState.RUNNING) return checkpoint;
        return effective;
    }

    private CatalogIntent readIntent(String id, boolean hasArtifact, PipelineCatalogState stateFacts) {
        try {
            if (stateFacts != null) {
                if (stateFacts.intentFailure().isPresent()) throw stateFacts.intentFailure().orElseThrow();
                return new CatalogIntent(stateFacts.desired().orElse(null), null);
            }
            return new CatalogIntent(hasArtifact ? desired.read(id).orElse(null) : null, null);
        } catch (TapstateException unreadable) {
            if (unreadable.code() != IoError.DOCUMENT_UNREADABLE) {
                throw unreadable;
            }
            LOG.log(Level.WARNING, "Unable to read desired state for Pipeline {0}", id);
            return new CatalogIntent(null,
                    new PipelineCatalogItem.IntentReadFailure(unreadable.code().code(), unreadable.args()));
        }
    }

    private static PipelineCatalogItem.DisplayState runtimeState(PipelineState engineState, PipelineStatus observation,
            PipelineCatalogState stateFacts, PipelineState checkpoint, PipelineState effective,
            PipelineCatalogItem.DisplayState state) {
        // The current failure and the later intent projection remain separate even if STOP arrived
        // after diagnosis. Converting this item must not infer runtime truth from the UI transition.
        if (engineState != null) return asDisplayState(engineState);
        if (observation == null && stateFacts != null && stateFacts.scope() != null
                && !stateFacts.runningObservationUnavailable()
                && (checkpoint == PipelineState.RUNNING || checkpoint == PipelineState.PAUSED)) return state;
        if (observation != null && observation.observedState() != effective) {
            return asDisplayState(observation.state());
        }
        return checkpoint == null ? state : asDisplayState(checkpoint);
    }

    private static PipelineCatalogItem.DisplayState asDisplayState(PipelineState state) {
        return state == null ? null : PipelineCatalogItem.DisplayState.valueOf(state.name());
    }

    private record CatalogIntent(DesiredState desired, PipelineCatalogItem.IntentReadFailure failure) {
    }

    private static PipelineCatalogItem.StopState stopState(DesiredState intent,
            PipelineCatalogState facts, PipelineCatalogItem.DisplayState state, boolean recovering) {
        if (recovering || intent == null || intent.targetState() != PipelineState.STOPPED) return null;
        if (intent.purgeState()) return PipelineCatalogItem.StopState.CLEARING;
        if (facts == null || facts.checkpoint() != PipelineState.STOPPED
                || state != PipelineCatalogItem.DisplayState.STOPPED) return null;
        return facts.completedStopClear() ? PipelineCatalogItem.StopState.CLEARED : PipelineCatalogItem.StopState.RETAINED;
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
            PipelineState desired, PipelineState observed, boolean intentReadable) {
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
        if (!intentReadable) {
            return null;
        }
        return PipelineCatalogItem.DisplayState.NEW;
    }
}
