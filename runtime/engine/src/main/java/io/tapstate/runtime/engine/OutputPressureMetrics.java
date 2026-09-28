package io.tapstate.runtime.engine;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.Stage;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** One processor's observed output refusals, including quota, backoff and scheduling delays. */
final class OutputPressureMetrics {

    private static final HistogramBounds BOUNDS = HistogramBounds.PROCESS_DURATION;

    private final Stage stage;
    private final LongSupplier nanoClock;
    private final long[] buckets = new long[BOUNDS.buckets()];
    private final Object lock = new Object();
    private volatile long scope;
    private volatile boolean enabled;
    private volatile boolean waiting;
    private long fromNanos;
    private long sinceMillis;
    private long refused;
    private long completed;
    private long sumNanos;

    OutputPressureMetrics(Stage stage, LongSupplier nanoClock) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    void startScope(long sinceMillis) {
        synchronized (lock) {
            this.sinceMillis = sinceMillis;
            refused = 0;
            completed = 0;
            sumNanos = 0;
            fromNanos = 0;
            waiting = false;
            Arrays.fill(buckets, 0);
            enabled = true;
            scope++;
        }
    }

    /** A cancelled or stopped attempt is not a successful retry interval. */
    void closeScope() {
        synchronized (lock) {
            enabled = false;
            waiting = false;
            fromNanos = 0;
            scope++;
        }
    }

    long scope() {
        return scope;
    }

    /** Called once after an ordinary offer returns; thrown offers are not false outcomes. */
    void observed(long offeredScope, boolean accepted) {
        if (offeredScope != scope || !enabled || accepted && !waiting) {
            return;
        }
        // Offers have one writer. Scope closure and collection may run elsewhere. Capture the boundary
        // before acquiring the short counter lock so collection itself does not extend a retry interval.
        boolean boundary = accepted || !waiting;
        long at = boundary ? nanoClock.getAsLong() : 0;
        synchronized (lock) {
            if (offeredScope != scope || !enabled) {
                return;
            }
            if (!accepted) {
                refused++;
                if (!waiting) {
                    fromNanos = at;
                    waiting = true;
                }
            } else if (waiting) {
                long elapsed = Math.max(0, at - fromNanos);
                completed++;
                sumNanos += elapsed;
                buckets[bucketOf(elapsed / 1_000_000_000.0)]++;
                waiting = false;
                fromNanos = 0;
            }
        }
    }

    /** The collection path alone copies one consistent tuple and its fixed bucket array. */
    Snapshot snapshot() {
        synchronized (lock) {
            return enabled ? new Snapshot(scope, sinceMillis, refused, completed, sumNanos, buckets.clone()) : null;
        }
    }

    record Snapshot(long scope, long sinceMillis, long refused, long completed, long sumNanos, long[] buckets) { }

    void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
        Snapshot reading = snapshot();
        if (reading == null) {
            return;
        }
        collect(collection, descriptor, OutputPressureMetricNames.Kind.READY, ProbeUnit.COUNT, 1);
        collect(collection, descriptor, OutputPressureMetricNames.Kind.SINCE, ProbeUnit.MS, reading.sinceMillis());
        collect(collection, descriptor, OutputPressureMetricNames.Kind.REFUSED, ProbeUnit.COUNT, reading.refused());
        collect(collection, descriptor, OutputPressureMetricNames.Kind.COUNT, ProbeUnit.COUNT, reading.completed());
        collect(collection, descriptor, OutputPressureMetricNames.Kind.SUM_NANOS, ProbeUnit.NS, reading.sumNanos());
        for (int index = 0; index < reading.buckets().length; index++) {
            collection.collect(descriptor.copy().withTag(MetricTags.USER, "true"),
                    OutputPressureMetricNames.bucketName(stage, index), ProbeLevel.INFO, ProbeUnit.COUNT,
                    reading.buckets()[index]);
        }
    }

    private void collect(MetricsCollectionContext collection, MetricDescriptor descriptor,
            OutputPressureMetricNames.Kind kind, ProbeUnit unit, long value) {
        collection.collect(descriptor.copy().withTag(MetricTags.USER, "true"),
                OutputPressureMetricNames.name(stage, kind), ProbeLevel.INFO, unit, value);
    }

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
