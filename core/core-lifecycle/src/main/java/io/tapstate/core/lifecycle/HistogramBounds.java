package io.tapstate.core.lifecycle;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The bucket bounds every histogram instrument publishes with, one entry per instrument, in seconds.
 *
 * <p>A histogram measures its quantity's range only as well as its buckets cover it. Bounds chosen by
 * whatever library happened to be in the path describe that library's idea of the range — an HTTP
 * library's default runs from a few milliseconds to ten seconds — and a data pipeline's delivery delay
 * runs from well under a second to the better part of an hour while a load catches up. Under the wrong
 * bounds every observation lands in the last bucket, which reads exactly like a quantity that never
 * varies. So the bounds are decided here, per instrument, and a distribution built with any other set is
 * refused where its fact is built ({@link MetricFact}).
 *
 * <p>Two instruments, two sets, because their ranges differ by four orders of magnitude at the low end: a
 * stage processes one item in microseconds to milliseconds and a batch in milliseconds to seconds, while a
 * row's delivery is measured from the moment the source produced it. Sharing a set would leave one of the
 * two with its whole population in three buckets.
 *
 * <p>Both sets step by a factor of two to two and a half so that a percentile read off them is off by at
 * most that factor, and both end where the quantity stops being a distribution and starts being an
 * incident: a row an hour late and a stage ten seconds on one item are each something a gauge or an alert
 * should be reporting, not a bucket.
 */
public enum HistogramBounds {

    /** How long a row took from being produced at the source to being confirmed at the target. */
    RECORD_DELIVERY_DURATION("tapstate.pipeline.record.delivery.duration", List.of(
            0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0)),

    /** How long one stage of the graph spent on one unit of its work. */
    PROCESS_DURATION("tapstate.pipeline.process.duration", List.of(
            0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            5.0, 10.0)),

    /**
     * How long one commit group of operator state took to become durable: one store transaction, from the
     * moment it was issued to the moment the store acknowledged it. Starts where a local commit does and
     * ends where a commit has stopped being a cost and become a stall.
     */
    STATE_COMMIT_DURATION("tapstate.pipeline.state.commit.duration", List.of(
            0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)),

    /**
     * How many keys one commit group carried. Powers of two up to the largest group the writer will form,
     * so a pipeline committing one key at a time and one committing in full groups land at opposite ends.
     */
    STATE_COMMIT_KEYS("tapstate.pipeline.state.commit.keys", "{key}", List.of(
            1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1000.0)),

    /**
     * How many store round trips one commit group cost, every command the store sent for it included.
     * Finest around twenty, the ceiling a group is held to, so a group just over it is told apart from one
     * just under.
     */
    STATE_COMMIT_ROUND_TRIPS("tapstate.pipeline.state.commit.round_trips", "{round_trip}", List.of(
            1.0, 2.0, 4.0, 8.0, 12.0, 16.0, 20.0, 24.0, 32.0, 48.0, 64.0));

    /** The unit every duration histogram here is measured in; their bounds are in it. */
    public static final String UNIT = "s";

    private final String instrument;
    private final String unit;
    private final List<Double> bounds;

    HistogramBounds(String instrument, List<Double> bounds) {
        this(instrument, UNIT, bounds);
    }

    HistogramBounds(String instrument, String unit, List<Double> bounds) {
        this.instrument = instrument;
        this.unit = unit;
        this.bounds = bounds;
    }

    /** The unit this instrument's bounds are given in: seconds for a duration, a count's own unit otherwise. */
    public String unit() {
        return unit;
    }

    /** The bounds registered for {@code instrument}, or empty for a name that publishes no distribution. */
    public static Optional<HistogramBounds> forInstrument(String instrument) {
        return Arrays.stream(values()).filter(entry -> entry.instrument.equals(instrument)).findFirst();
    }

    /** The metric name these bounds belong to. */
    public String instrument() {
        return instrument;
    }

    /** The explicit upper bounds, ascending; the last bucket holds everything above the highest. */
    public List<Double> bounds() {
        return bounds;
    }

    /** How many buckets a distribution over these bounds has: one more than there are bounds. */
    public int buckets() {
        return bounds.size() + 1;
    }

    /** A distribution over these bounds, which is the only shape a point of this instrument may carry. */
    public HistogramValue value(long count, double sum, List<Long> bucketCounts) {
        return new HistogramValue(count, sum, bounds, bucketCounts);
    }
}
