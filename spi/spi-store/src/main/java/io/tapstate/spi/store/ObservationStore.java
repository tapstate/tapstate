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
 * <p>A scope-bound business view also checks its fixed physical state and the original live claim on
 * every mutation. Last-write-wins applies only inside that admitted scope; a stale scope or run cannot
 * overwrite its replacement. Ordinary business views refuse physical removal.
 */
public interface ObservationStore {

    /** Upserts the observation for its pipeline id (last write wins). */
    void save(Observation observation);

    /** Returns the current observation for a pipeline, or empty if none has been published. */
    Optional<Observation> read(String pipelineId);

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
