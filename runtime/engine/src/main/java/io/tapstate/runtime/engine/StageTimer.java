package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
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
    private volatile boolean active;
    private boolean entered;
    private final int expected;
    private final int members;
    private final long[] buckets = new long[BOUNDS.buckets()];

    StageTimer(Stage stage, StageGauge gauge, LongSupplier nanoClock, long countingSinceMillis) {
        this(stage, gauge, nanoClock, countingSinceMillis, 0, 0);
    }

    private StageTimer(Stage stage, StageGauge gauge, LongSupplier nanoClock, long countingSinceMillis,
            int expected, int members) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.gauge = Objects.requireNonNull(gauge, "gauge");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.countingSinceMillis = countingSinceMillis;
        this.expected = expected;
        this.members = members;
    }

    /**
     * The timer a processor of {@code stage} uses inside {@code context}: one that writes into the job's
     * statistics when there is a job to write into, and one that counts for nobody when there is not — a
     * processor driven by hand has no job, and asking for a statistic handle there fails outright.
     */
    public static StageTimer of(Stage stage, Processor.Context context) {
        boolean inAJob = context != null && context.hazelcastInstance() != null;
        java.util.Set<String> single = inAJob && context.jobConfig() != null
                ? context.jobConfig().getArgument(StageWorkDag.SINGLE_ARGUMENT) : null;
        boolean pinned = single != null && single.contains(context.vertexName());
        // A single-business vertex is compiled at local parallelism one; its other member slots are inert.
        int expected = !inAJob ? 0 : pinned
                ? context.localParallelism() == 1 && context.totalParallelism() == context.memberCount() ? 1 : 0
                : context.totalParallelism();
        return new StageTimer(stage, inAJob ? new JetStageGauge() : StageGauge.none(),
                System::nanoTime, System.currentTimeMillis(),
                expected, inAJob ? context.memberCount() : 0);
    }

    /** A timer that counts and reports to nobody, for a processor not yet initialised. */
    public static StageTimer none(Stage stage) {
        return new StageTimer(stage, StageGauge.none(), System::nanoTime, System.currentTimeMillis());
    }

    /** The moment a unit begins, to be handed back to {@link #end(long)}. */
    public long begin() {
        enter();
        active = true;
        gauge.active(stage, 1);
        return nanoClock.getAsLong();
    }

    /** Starts a source read whose result may be empty; only a known row makes it active business work. */
    public long beginInactive() {
        enter();
        gauge.active(stage, 0);
        return nanoClock.getAsLong();
    }

    private void enter() {
        if (entered) {
            throw new IllegalStateException("a processor cannot enter a second business unit before leaving its first");
        }
        entered = true;
    }

    /** Activates a source unit immediately before projecting its first known item. */
    public void activate() {
        if (!entered) {
            throw new IllegalStateException("business work must be entered before activating");
        }
        if (!active) {
            active = true;
            gauge.active(stage, 1);
        }
    }

    /** Ends the unit begun at {@code startedNanos}, counting it into the distribution and reporting. */
    public void end(long startedNanos) {
        leave();
        long nanos = Math.max(0L, nanoClock.getAsLong() - startedNanos);
        count++;
        sumNanos += nanos;
        buckets[bucketOf(nanos / 1_000_000_000.0)]++;
        gauge.took(stage, count, sumNanos, buckets, countingSinceMillis);
    }

    /** Releases an empty source poll without calling it a completed business unit. */
    public void discard(long startedNanos) {
        leave();
    }

    private void leave() {
        if (!entered) {
            throw new IllegalStateException("business work must be entered before leaving");
        }
        entered = false;
        active = false;
        gauge.active(stage, 0);
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

    /** The state of this business-work boundary, also observable when driven outside a job. */
    public boolean isActive() {
        return active;
    }

    /** Copies one collection snapshot independently of whether this processor has received its first row. */
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
        if (expected <= 0 || members <= 0) {
            return;
        }
        long current = active ? 1 : 0;
        String prefix = JetStageGauge.PREFIX + stage.attributeValue();
        collectWork(collection, descriptor, prefix + JetStageGauge.ACTIVE, current);
        collectWork(collection, descriptor, prefix + JetStageGauge.EXPECTED, expected);
        collectWork(collection, descriptor, prefix + JetStageGauge.MEMBERS, members);
        collectWork(collection, descriptor, prefix + JetStageGauge.READY, 1);
    }

    private static void collectWork(MetricsCollectionContext collection, MetricDescriptor descriptor,
            String name, long value) {
        collection.collect(descriptor.copy().withTag(MetricTags.USER, "true"), name,
                ProbeLevel.INFO, ProbeUnit.COUNT, value);
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
