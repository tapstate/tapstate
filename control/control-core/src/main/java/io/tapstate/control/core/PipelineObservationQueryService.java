package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ExecutionPlans;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

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
    private final ExecutionPlans plans;
    private final Supplier<List<String>> members;

    public PipelineObservationQueryService(ArtifactQueryService artifacts, ObservationStore observations) {
        this(artifacts, observations, ExecutionPlans.NONE);
    }

    /** As above, answering a status with the plan its pipeline's current run was submitted on, from {@code plans}. */
    public PipelineObservationQueryService(ArtifactQueryService artifacts, ObservationStore observations,
            ExecutionPlans plans) {
        this(artifacts, observations, plans, List::of);
    }

    /**
     * As above, also naming the members of the cluster - {@code members}, by stable id - the plan was not worked
     * out for.
     */
    public PipelineObservationQueryService(ArtifactQueryService artifacts, ObservationStore observations,
            ExecutionPlans plans, Supplier<List<String>> members) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.plans = Objects.requireNonNull(plans, "plans");
        this.members = Objects.requireNonNull(members, "members");
    }

    /**
     * The pipeline's lifecycle state, with the coded reason its run died when there is one, the plan its current
     * run was submitted on where one is recorded, and the members of the cluster that plan was not worked out for.
     */
    public PipelineStatus status(String pipelineId) {
        PipelineStatus status = lifecycleStatus(pipelineId);
        ExecutionPlan plan = plans.current(List.of(pipelineId)).get(pipelineId);
        return new PipelineStatus(status.pipelineId(), status.state(), status.failure(), status.observedAt(),
                plan, plan == null ? List.of() : plan.notPlannedFor(members.get()));
    }

    /**
     * The pipeline's status as {@link #status} answers it, without the plan: what a reader following the state
     * as it changes asks for on every poll. The plan changes only when a new run is submitted, so reading it on
     * every poll would be a read thrown away each time.
     */
    public PipelineStatus lifecycleStatus(String pipelineId) {
        Observation observation = require(pipelineId);
        return new PipelineStatus(observation.pipelineId(), observation.state(), observation.failure(),
                observation.observedAt());
    }

    /** Returns the latest status when an observation exists, without turning an unobserved pipeline into an error. */
    public Optional<PipelineStatus> findStatus(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return observations.read(pipelineId)
                .map(observation -> new PipelineStatus(observation.pipelineId(), observation.state(),
                        observation.failure(), observation.observedAt()));
    }

    /**
     * The pipeline's open map of run statistics, the facts those statistics were measured as, and, per
     * table, the one source position it records: how far the target has confirmed writes. The stored
     * projection carries that position under a name that does not say which of the four positions it is;
     * this face gives it back its name, because the face is where somebody decides whether a run is stuck.
     */
    public PipelineMetrics metrics(String pipelineId) {
        Observation observation = require(pipelineId);
        return new PipelineMetrics(observation.pipelineId(), observation.metrics(), observation.positions(),
                observation.facts());
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
