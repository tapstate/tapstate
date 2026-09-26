package io.tapstate.control.core;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Resolves a latest observation only when its internal owner is the current pipeline execution. */
public final class CurrentObservationReader {

    private final ArtifactStore artifacts;
    private final ExecutionGenerationStore generations;
    private final ObservationStore observations;
    private final String clusterId;

    public CurrentObservationReader(ArtifactStore artifacts, ExecutionGenerationStore generations,
            ObservationStore observations, String clusterId) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    }

    public Optional<Observation> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (artifacts.get(pipelineId).filter(artifact -> "pipeline".equals(artifact.kind())).isEmpty()) {
            return Optional.empty();
        }
        Optional<String> incarnation = artifacts.pipelineIncarnationId(pipelineId);
        if (incarnation.isEmpty()) {
            // Upgrade-era resources retain their old pipeline-id interpretation until assigned an identity.
            return observations.readStored(pipelineId)
                    .filter(stored -> stored.scope().isEmpty()).map(ObservationStore.Stored::observation);
        }
        OptionalLong generation = generations.currentGeneration(clusterId, pipelineId);
        if (generation.isEmpty()) {
            return Optional.empty();
        }
        ObservationStore.Scope expected = new ObservationStore.Scope(incarnation.get(), generation.getAsLong());
        return observations.readStored(pipelineId)
                .filter(stored -> stored.scope().filter(expected::equals).isPresent())
                .map(ObservationStore.Stored::observation);
    }
}
