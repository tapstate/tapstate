package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Processor;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Times a stage's units of work and keeps their distribution over the registered bounds. A processor
 * takes a start with {@link #begin()} before a unit and hands it back with {@link #end(long)} after, and
 * the distribution so far goes to the gauge each time — so a job's statistics carry it without a second
 * channel to keep alive.
 *
 * <p><strong>What a unit is belongs to the stage that times it, and is written down with it.</strong> A
 * transform times a row through its port; a nest, a join and a sink time one drain of what arrived; a
 * source times one read of its ring that produced something. Idle calls — a drain offered nothing, a read
 * that found the ring empty — are not units and are not timed: a distribution swamped by zero-length
 * idles would report every stage as instantaneous, which is exactly the shape that hides a slow one.
 *
 * <p>The clock is monotonic and in nanoseconds; the sum is kept in nanoseconds so it never drifts by
 * rounding and converts once on the way out. Outside a running job the gauge reads nothing and this
 * still counts, so a processor driven by hand can be shown to time its work.
 *
 * <p>The reading goes to the gauge as the numbers behind it rather than as a distribution object: a unit
 * of work moves three of the nineteen numbers, and assembling the object here would allocate a list and
 * box seventeen counts per row to carry figures this timer already holds. {@link #value()} assembles it
 * for a reader, which is not the data path.
 */
public final class StageTimer {

    private static final HistogramBounds BOUNDS = HistogramBounds.PROCESS_DURATION;

    private final Stage stage;
    private final StageGauge gauge;
    private final LongSupplier nanoClock;
    private final long countingSinceMillis;
    private long count;
    private long sumNanos;
    private final long[] buckets = new long[BOUNDS.buckets()];

    StageTimer(Stage stage, StageGauge gauge, LongSupplier nanoClock, long countingSinceMillis) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.gauge = Objects.requireNonNull(gauge, "gauge");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.countingSinceMillis = countingSinceMillis;
    }

    /**
     * The timer a processor of {@code stage} uses inside {@code context}: one that writes into the job's
     * statistics when there is a job to write into, and one that counts for nobody when there is not — a
     * processor driven by hand has no job, and asking for a statistic handle there fails outright.
     */
    public static StageTimer of(Stage stage, Processor.Context context) {
        boolean inAJob = context != null && context.hazelcastInstance() != null;
        return new StageTimer(stage, inAJob ? new JetStageGauge() : StageGauge.none(),
                System::nanoTime, System.currentTimeMillis());
    }

    /** A timer that counts and reports to nobody, for a processor not yet initialised. */
    public static StageTimer none(Stage stage) {
        return new StageTimer(stage, StageGauge.none(), System::nanoTime, System.currentTimeMillis());
    }

    /** The moment a unit begins, to be handed back to {@link #end(long)}. */
    public long begin() {
        return nanoClock.getAsLong();
    }

    /** Ends the unit begun at {@code startedNanos}, counting it into the distribution and reporting. */
    public void end(long startedNanos) {
        long nanos = Math.max(0L, nanoClock.getAsLong() - startedNanos);
        count++;
        sumNanos += nanos;
        buckets[bucketOf(nanos / 1_000_000_000.0)]++;
        gauge.took(stage, count, sumNanos, buckets, countingSinceMillis);
    }

    /** The distribution so far. */
    public HistogramValue value() {
        List<Long> counts = new ArrayList<>(buckets.length);
        for (long bucket : buckets) {
            counts.add(bucket);
        }
        return BOUNDS.value(count, sumNanos / 1_000_000_000.0, counts);
    }

    public Stage stage() {
        return stage;
    }

    /** The first bucket whose upper bound the value does not exceed; the last bucket for everything above. */
    private static int bucketOf(double seconds) {
        List<Double> bounds = BOUNDS.bounds();
        for (int index = 0; index < bounds.size(); index++) {
            if (seconds <= bounds.get(index)) {
                return index;
            }
        }
        return bounds.size();
    }
}
