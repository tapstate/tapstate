package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.Observation;
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
 *
 * <p>A scope-bound business view isolates writes by the original execution and the measured
 * checkpoint epoch. Its read view chooses the observation that matches the current execution and
 * checkpoint. Ordinary business views refuse physical removal.
 */
public interface ObservationStore {

    /** Upserts the observation for its pipeline id (last write wins). */
    void save(Observation observation);

    /** Publishes a sample measured against the given checkpoint, without adopting a newer epoch. */
    default void save(Observation observation, long checkpointEpoch) { save(observation); }

    /** Final data measured immediately before the bounded execution completed. */
    default Optional<Observation> beforeCompletion(String pipelineId, long checkpointEpoch) {
        return read(pipelineId);
    }

    /** Returns the current observation for a pipeline, or empty if none has been published. */
    Optional<Observation> read(String pipelineId);

    /** Reads a page of observations, omitting unpublished rows. */
    default java.util.Map<String, Observation> readAll(java.util.List<String> ids) {
        var result = new java.util.HashMap<String, Observation>();
        for (var id : ids) read(id).ifPresent(value -> result.put(id, value));
        return java.util.Map.copyOf(result);
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
