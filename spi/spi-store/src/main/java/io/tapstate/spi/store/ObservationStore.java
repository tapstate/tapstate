package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.Observation;
import java.util.Objects;
import java.util.Optional;

/**
 * The per-pipeline observation store: persists one observation doc per pipeline — the latest read-only
 * projection of its state, metrics and snapshot progress. A pure interface over the observation model
 * in the core ring (rule R2); it exposes the persistence surface only.
 *
 * <p>An observation is a plain latest-state projection, not a fenced transition and not a time series,
 * so it is a plain upsert by pipeline id (last write wins) — the same shape as the desired-intent
 * store. The runtime publishes it after converging; the control read faces read it. {@link #save}
 * upserts the observation for its pipeline; {@link #read} returns the current observation for a
 * pipeline, or empty when none has been published.
 */
public interface ObservationStore {

    /** Internal owner of a published observation; neither field is part of the public observation. */
    record Scope(String pipelineIncarnationId, long executionGeneration) {
        public Scope {
            Objects.requireNonNull(pipelineIncarnationId, "pipelineIncarnationId");
            if (pipelineIncarnationId.isBlank() || executionGeneration <= 0) {
                throw new IllegalArgumentException("an observation scope needs an incarnation and positive generation");
            }
        }
    }

    /** Stored projection and its optional internal owner; absence means a legacy unscoped document. */
    record Stored(Observation observation, Optional<Scope> scope) {
        public Stored {
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(scope, "scope");
        }
    }

    /** Upserts the observation for its pipeline id (last write wins). */
    void save(Observation observation);

    /**
     * Conditionally stores the latest observation for one execution. Returns false for an older
     * generation, a different incarnation at the same generation, or non-advancing observation time.
     * A newer generation may replace an older one after a resource is recreated. Implementations that do
     * not support scoped writes must fail closed rather than silently publish without a fence.
     */
    default boolean saveScoped(Observation observation, Scope scope) {
        throw new UnsupportedOperationException("scoped observation writes are unavailable");
    }

    /** Returns the current observation for a pipeline, or empty if none has been published. */
    Optional<Observation> read(String pipelineId);

    /**
     * Reads the stored owner without deciding whether legacy data belongs to the current artifact.
     * Callers compare the scope with authoritative artifact and execution identity before projection.
     */
    default Optional<Stored> readStored(String pipelineId) {
        return read(pipelineId).map(observation -> new Stored(observation, Optional.empty()));
    }

    /**
     * Removes a pipeline's observation, so the read faces stop projecting a pipeline that no longer
     * exists.
     *
     * <p>Removing an observation that is not there is a no-op, not an error: a pipeline that never
     * converged published none, and the caller reclaiming after a removal should not have to ask first —
     * nor should a second attempt behave differently from the first.
     */
    void delete(String pipelineId);
}
