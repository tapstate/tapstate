package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.HistogramBounds;
import java.util.HashMap;
import java.util.Map;

/** Fixed-size job statistics for the sink batch boundary; no table or record value enters a metric name. */
final class JetSinkUseGauge implements SinkUseGauge {

    static final String PREFIX = "sink.batch.";
    static final String ISSUED = PREFIX + "issued";
    static final String RECORDS = PREFIX + "records";
    static final String LARGEST = PREFIX + "records.max";
    static final String PENDING = PREFIX + "pending";
    static final String LIMIT = PREFIX + "limit";
    static final String BACKPRESSURED = PREFIX + "backpressured";
    static final String SINCE = PREFIX + "since";
    static final String WRITE = PREFIX + "write.";
    static final String WAIT = PREFIX + "backpressure.";
    static final String COUNT = "count";
    static final String SUM_MICROS = "sumMicros";
    static final String BUCKET = "bucket.";

    private final Map<String, JobStatistic> parts = new HashMap<>();
    private final Distribution writes = new Distribution(WRITE, HistogramBounds.SINK_BATCH_WRITE_DURATION);
    private final Distribution waits = new Distribution(WAIT, HistogramBounds.SINK_BACKPRESSURE_DURATION);

    @Override
    public void issued(long batches, long records, long largestBatch, int pending, int limit, long sinceMillis) {
        part(ISSUED).set(batches);
        part(RECORDS).set(records);
        part(LARGEST).set(largestBatch);
        part(PENDING).set(pending);
        part(LIMIT).set(limit);
        part(SINCE).set(sinceMillis);
    }

    @Override
    public void settled(long writeNanos, int pending) {
        writes.record(writeNanos);
        part(PENDING).set(pending);
    }

    @Override
    public void backpressured(boolean active) {
        part(BACKPRESSURED).set(active ? 1L : 0L);
    }

    @Override
    public void waited(long waitNanos) {
        waits.record(waitNanos);
    }

    @Override
    public boolean readableOnlyOnAJobThread() {
        return true;
    }

    private JobStatistic part(String name) {
        return parts.computeIfAbsent(name, JobStatistic::new);
    }

    private final class Distribution {
        private final String prefix;
        private final HistogramBounds bounds;
        private final long[] buckets;
        private long count;
        private long sumNanos;

        Distribution(String prefix, HistogramBounds bounds) {
            this.prefix = prefix;
            this.bounds = bounds;
            this.buckets = new long[bounds.buckets()];
        }

        void record(long nanos) {
            count++;
            sumNanos += nanos;
            double seconds = nanos / 1_000_000_000.0;
            int bucket = 0;
            while (bucket < bounds.bounds().size() && seconds > bounds.bounds().get(bucket)) {
                bucket++;
            }
            buckets[bucket]++;
            part(prefix + COUNT).set(count);
            part(prefix + SUM_MICROS).set(Math.round(sumNanos / 1_000.0));
            for (int index = 0; index < buckets.length; index++) {
                part(prefix + BUCKET + index).set(buckets[index]);
            }
        }
    }
}
