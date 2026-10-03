package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WorkloadClaimType;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Cold identity reads qualify an existing execution without allocating another generation. */
final class ObservationScopeRecovery {
    record Qualified(ObservationStore.Scope scope, ObservationStore.Stored stored, CheckpointDoc checkpoint) { }

    record Owner(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision) {
        static Owner of(WorkloadClaim claim) {
            return claim == null ? null : new Owner(claim.key(), claim.owner(), claim.claimGeneration(),
                    claim.executionGeneration(), claim.topologyRevision());
        }
    }

    private final ArtifactStore artifacts;
    private final ExecutionGenerationStore generations;
    private final ObservationStore observations;
    private final StateStore state;
    private final String clusterId;

    ObservationScopeRecovery(ArtifactStore artifacts, ExecutionGenerationStore generations,
            ObservationStore observations, StateStore state, String clusterId) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.state = Objects.requireNonNull(state, "state");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    }

    Optional<Qualified> resolve(String pipelineId) {
        Optional<ObservationStore.Scope> expected = authority(pipelineId);
        if (expected.isEmpty()) {
            return Optional.empty();
        }
        ObservationStore.Scope scope = expected.orElseThrow();
        Optional<ObservationStore.Stored> saved = observations.readStored(pipelineId)
                .filter(stored -> stored.scope().filter(scope::equals).isPresent())
                .filter(stored -> pipelineId.equals(stored.observation().pipelineId()))
                .filter(stored -> stored.observation().state() != PipelineState.NEW);
        if (saved.isEmpty()) {
            return Optional.empty();
        }
        return state.read(pipelineId)
                .filter(checkpoint -> pipelineId.equals(checkpoint.pipelineId()))
                .filter(checkpoint -> StateJson.parse(checkpoint.stateJson()) != PipelineState.NEW)
                .filter(checkpoint -> authority(pipelineId).filter(scope::equals).isPresent())
                .map(checkpoint -> new Qualified(scope, saved.orElseThrow(), checkpoint));
    }

    boolean unchanged(String pipelineId, Qualified qualified) {
        return unchanged(pipelineId, qualified.scope(), qualified.checkpoint());
    }

    private boolean unchanged(String pipelineId, ObservationStore.Scope scope, CheckpointDoc checkpoint) {
        return state.read(pipelineId).filter(checkpoint::equals).isPresent()
                && authority(pipelineId).filter(scope::equals).isPresent();
    }

    /** An already admitted local failure needs no previous observation or fabricated native baseline. */
    boolean failedAttemptCurrent(String pipelineId, ObservationStore.Scope scope, CheckpointDoc checkpoint,
            Owner captured) {
        return scope != null && checkpoint != null && pipelineId.equals(checkpoint.pipelineId())
                && StateJson.parse(checkpoint.stateJson()) == PipelineState.FAILED
                && matchesOwner(pipelineId, scope, captured) && unchanged(pipelineId, scope, checkpoint);
    }

    boolean matchesOwner(String pipelineId, Qualified qualified, Owner captured) {
        return matchesOwner(pipelineId, qualified.scope(), captured);
    }

    private boolean matchesOwner(String pipelineId, ObservationStore.Scope scope, Owner captured) {
        return captured == null || (clusterId.equals(captured.key().clusterId())
                && pipelineId.equals(captured.key().resourceId())
                && captured.key().type() == WorkloadClaimType.PIPELINE_ACTUATION
                && captured.executionGeneration() == scope.executionGeneration());
    }

    private Optional<ObservationStore.Scope> authority(String pipelineId) {
        if (artifacts.get(pipelineId).filter(resource -> "pipeline".equals(resource.kind())).isEmpty()) {
            return Optional.empty();
        }
        Optional<String> incarnation = artifacts.pipelineIncarnationId(pipelineId);
        if (incarnation.isEmpty()) {
            return Optional.empty();
        }
        OptionalLong generation = generations.currentGeneration(clusterId, pipelineId);
        if (generation.isEmpty() || generation.getAsLong() <= 0) {
            return Optional.empty();
        }
        return Optional.of(new ObservationStore.Scope(incarnation.orElseThrow(), generation.getAsLong()));
    }
}
