package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.RateSample;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The per-pipeline history of movement samples: one document per sample, appended on a fixed cadence and
 * kept for a bounded time, so that a rate and a delay can be drawn as lines. A pure interface over the
 * sample model in the core ring (rule R2); it exposes the persistence surface only.
 *
 * <p>This is the one store here that is a series and not a latest state. Two things follow. Nothing is
 * ever overwritten: {@link #append} adds and only adds. And nothing is ever trimmed by the writer:
 * samples leave by age, and the adapter behind this port is what makes them leave — a writer that trimmed
 * on its own way in would never trim the history of a pipeline that has stopped writing, which is exactly
 * the pipeline whose history has nobody else to bound it. {@link #retention} says how long a sample is
 * kept; how the adapter enforces it is the adapter's business, behind this port.
 *
 * <p>Reads are by one pipeline and one time range and nothing else. A read across pipelines, or across all
 * time, is a scan of a collection that grows by the minute, and no read face is allowed to depend on one.
 */
public interface RateHistoryStore {

    /** Adds one sample. Never overwrites: a second sample at the same instant is a second document. */
    void append(RateSample sample);

    /**
     * The samples of {@code pipelineId} taken from {@code from} up to and including {@code to}, oldest
     * first; empty when there are none in the range, which is also what a pipeline never sampled reads as.
     */
    List<RateSample> readBetween(String pipelineId, Instant from, Instant to);

    /**
     * Removes every sample of {@code pipelineId}, so a pipeline that no longer exists leaves no history.
     * Removing nothing is a no-op, not an error, for the reason the observation store's delete is one.
     */
    void deleteAll(String pipelineId);

    /** How long a sample is kept before the store lets it go. */
    Duration retention();
}
