package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.Stage;

/**
 * Where a processor reports how long its stage's units of work have taken: the distribution so far and
 * the moment it began timing. A seam for the reason {@link DeliveryGauge} is one — the reading is taken
 * inside the run and read outside it — and, like it, one that reads nothing when a processor is driven
 * by hand outside a job, since a job's own statistics can only be written from the job's threads.
 */
interface StageGauge {

    /**
     * Takes the reading: {@code stage}'s distribution as it stands, as the numbers it is made of, and the
     * epoch millisecond the timing began. Cumulative, the way every reading in a run's statistics is.
     *
     * <p>The numbers and not a {@code HistogramValue}, because this is called once per unit of work — for
     * a transform, once per row on the cooperative thread. Building the value object there would allocate
     * a list and box seventeen counts per row to carry numbers the caller already holds.
     *
     * <p>{@code bucketCounts} belongs to the caller and goes on changing after this returns: an
     * implementation reads it and does not keep it.
     */
    void took(Stage stage, long count, long sumNanos, long[] bucketCounts, long countingSinceMillis);

    /** A gauge nothing reads, for a processor driven outside a running job. */
    static StageGauge none() {
        return (stage, count, sumNanos, bucketCounts, countingSinceMillis) -> {
        };
    }
}
