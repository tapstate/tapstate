package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * What a pipeline's run reports about where in its graph time is spent: one distribution per stage of how
 * long the stage's units of work took, over the registered bounds, accumulated since the run began timing.
 *
 * <p>A distribution and not a total, because a total per stage answers "which stage is expensive" while
 * losing "is it expensive every time or once in a while", and those call for different fixes. The unit
 * a stage times is the stage's own — a row through a transform, a drain of arrivals through a nest or a
 * join, a batch issued by a sink, a read of the ring by a source — and is written down with the stage, so
 * a reader comparing two stages' distributions knows they are not comparing rows to rows.
 *
 * <p>Keyed by the stage's attribute value, which is a closed set: nothing arriving from the data can add
 * a stage. The start is the latest of the stages' own, for the reason the delivery reading takes the
 * latest of its sinks': a distribution that empties on a restart reads as a restart only while the start
 * moves with it.
 *
 * <p>An empty reading is a run with nothing to report — no live job, or stages that have timed nothing
 * yet — and a reading with distributions in it says what they accumulate from.
 */
public record StageReading(Map<String, HistogramValue> durationByStage, Instant countingSince) {

    /** A reading from a run reporting nothing. */
    public static final StageReading NONE = new StageReading(Map.of(), null);

    public StageReading {
        durationByStage = durationByStage == null ? Map.of() : Map.copyOf(durationByStage);
        for (String stage : durationByStage.keySet()) {
            if (!Stage.attributeValues().contains(stage)) {
                throw new IllegalArgumentException("'" + stage + "' is not a stage of the graph; the stages are "
                        + Stage.attributeValues());
            }
        }
        if (!durationByStage.isEmpty() && countingSince == null) {
            throw new IllegalArgumentException(
                    "a reading with distributions in it says what it accumulated them from: a distribution"
                            + " without a start hands every consumer a stream in which a restart and a"
                            + " decrease are the same observation");
        }
    }

    /** When the timing behind these distributions began, absent for a reading that timed nothing. */
    public Optional<Instant> start() {
        return Optional.ofNullable(countingSince);
    }

    public boolean isEmpty() {
        return durationByStage.isEmpty();
    }
}
