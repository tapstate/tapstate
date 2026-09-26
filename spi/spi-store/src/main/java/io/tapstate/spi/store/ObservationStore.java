package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.Observation;
import java.util.Objects;
import java.util.Optional;

/**
 * The per-pipeline observation store: persists one observation doc per pipeline — the latest read-only
 * projection of its state, metrics and snapshot progress. A pure interface over the observation model
 * in the core ring (rule R2); it exposes the persistence surface only.
 *
 * <p>An observation is a latest-state projection, not a time series. Scoped writes keep one document
 * by pipeline id and conditionally replace it only for a newer execution or observation time. A
 * control reader compares its stored owner with the current artifact and execution before returning it.
 * The plain save/read methods retain the pre-identity compatibility path.
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

    /** Legacy unscoped upsert; new executions use the conditional scoped write. */
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

    /** Unconditional legacy removal; artifact cleanup must use an owner-matched method below. */
    void delete(String pipelineId);

    /** Removes only the deleted incarnation's latest document, leaving a recreated resource untouched. */
    default void deleteIncarnation(String pipelineId, String incarnationId) {
        throw new UnsupportedOperationException("scoped observation cleanup is unavailable");
    }

    /** Removes only an observation written before an internal execution owner existed. */
    default void deleteLegacy(String pipelineId) {
        throw new UnsupportedOperationException("legacy observation cleanup is unavailable");
    }
}
