package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;

/**
 * One number in a job's own statistics, with the last value written through it remembered so that an
 * unchanged number is not written again.
 *
 * <p>The gauges that use this publish from the data path, and what they publish is a whole reading each
 * time: a stage's distribution is seventeen buckets, a count and a sum, and one unit of work moves three
 * of those nineteen numbers. A sink republishes every table's distribution when any batch settles, and a
 * batch touches one table of the fifty. So most of what a naive publish writes is the value that is
 * already there — for a transform, on the cooperative thread, once per row.
 *
 * <p>Not writing an unchanged value is safe because these are values and not deltas: a job's statistics
 * keep the last number set under a name until another is set, which is what already makes a count read
 * correctly through a lull in which nothing settles and nothing is published at all.
 */
final class JobStatistic {

    private final Metric metric;
    private long written;
    private boolean everWritten;

    JobStatistic(String name) {
        this.metric = Metrics.metric(name);
    }

    /** Writes {@code value} unless it is the value this statistic already carries. */
    void set(long value) {
        if (everWritten && written == value) {
            return;
        }
        metric.set(value);
        written = value;
        everWritten = true;
    }
}
