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
            5.0, 10.0));

    /** The one unit every histogram here is measured in; the bounds above are in it. */
    public static final String UNIT = "s";

    private final String instrument;
    private final List<Double> bounds;

    HistogramBounds(String instrument, List<Double> bounds) {
        this.instrument = instrument;
        this.bounds = bounds;
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
