package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The pipeline observation read side: the three store-backed read faces — status / metrics / snapshot —
 * each a projection of the one per-pipeline observation the runtime publishes. The read peer of the
 * lifecycle write side; it reads the observation store and never calls the runtime (control and runtime
 * meet only through the store). Every refusal is coded, so the same read serves a frontend with no
 * stderr/exit channel rather than a bare usage error.
 *
 * <p>A pipeline with no published observation is two different situations and they are answered by two
 * different codes. One is transient — the pipeline was applied and no convergence pass has reached it
 * yet — and a caller is entitled to wait it out; that is {@code monitor.no-observation}. The other is
 * permanent — no such pipeline was ever applied, usually a mistyped id — and waiting will never help;
 * that is {@code lifecycle.unknown-pipeline}, the same code the write side answers. Telling them apart
 * needs the artifact, which is why this read consults it: answering one code for both left a caller
 * spending its whole timeout on a typo and then blaming the data.
 */
public final class PipelineObservationQueryService {

    /** The stored-artifact kind a lifecycle verb can observe; any other kind is not a pipeline at all. */
    private static final String PIPELINE_KIND = "pipeline";

    private final ArtifactQueryService artifacts;
    private final ObservationStore observations;

    public PipelineObservationQueryService(ArtifactQueryService artifacts, ObservationStore observations) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
    }

    /** The pipeline's lifecycle state, with the coded reason its run died when there is one. */
    public PipelineStatus status(String pipelineId) {
        Observation observation = require(pipelineId);
        return new PipelineStatus(observation.pipelineId(), observation.state(), observation.failure());
    }

    /** Returns the latest status when an observation exists, without turning an unobserved pipeline into an error. */
    public Optional<PipelineStatus> findStatus(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return observations.read(pipelineId)
                .map(observation -> new PipelineStatus(observation.pipelineId(), observation.state(), observation.failure()));
    }

    /** The pipeline's open map of run statistics plus its per-table source positions. */
    public PipelineMetrics metrics(String pipelineId) {
        Observation observation = require(pipelineId);
        return new PipelineMetrics(observation.pipelineId(), observation.metrics(), observation.positions());
    }

    /** The pipeline's per-table initial-load progress. */
    public PipelineSnapshot snapshot(String pipelineId) {
        Observation observation = require(pipelineId);
        return new PipelineSnapshot(observation.pipelineId(), observation.snapshot());
    }

    private Observation require(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return observations.read(pipelineId).orElseThrow(() -> unobserved(pipelineId));
    }

    /**
     * Why the pipeline has no observation: no such pipeline was ever applied, or it was and has not
     * converged yet. "No such pipeline" covers two cases the same way: the id resolves to nothing at all,
     * and the id resolves to some other kind of resource (a source, a view, ...) — a lifecycle verb never
     * had a pipeline to converge either way, so both answer the permanent code rather than the transient
     * one a real pipeline mid-convergence gets. The artifact is consulted only on this path, so a normal
     * read costs one store call as before.
     */
    private TapstateException unobserved(String pipelineId) {
        boolean isPipeline = artifacts.get(pipelineId)
                .map(artifact -> PIPELINE_KIND.equals(artifact.kind()))
                .orElse(false);
        if (!isPipeline) {
            return new TapstateException(
                    LifecycleError.UNKNOWN_PIPELINE, Map.of("pipeline", pipelineId), null);
        }
        return new TapstateException(MonitorError.NO_OBSERVATION, Map.of("pipeline", pipelineId), null);
    }
}
