package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;

/**
 * Where a processor reports how long its stage's units of work have taken: the distribution so far and
 * the moment it began timing. A seam for the reason {@link DeliveryGauge} is one — the reading is taken
 * inside the run and read outside it — and, like it, one that reads nothing when a processor is driven
 * by hand outside a job, since a job's own statistics can only be written from the job's threads.
 */
interface StageGauge {

    /**
     * Takes the reading: {@code stage}'s distribution as it stands, over the registered bounds, and the
     * epoch millisecond the timing began. Cumulative, the way every reading in a run's statistics is.
     */
    void took(Stage stage, HistogramValue distribution, long countingSinceMillis);

    /** A gauge nothing reads, for a processor driven outside a running job. */
    static StageGauge none() {
        return (stage, distribution, countingSinceMillis) -> {
        };
    }
}
