package io.tapstate.runtime.srs;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.PayloadBytes;
import java.util.HashMap;
import java.util.Map;

/** Counts this job's source inputs, including replay, through the job's existing statistics channel. */
final class SourceArrivals {
    private final boolean publishing;
    private final long began = System.currentTimeMillis();
    private final Map<String, Long> totals = new HashMap<>();
    private final Map<String, Metric> gauges = new HashMap<>();
    private Metric since;

    private SourceArrivals(boolean publishing) { this.publishing = publishing; }

    static SourceArrivals of(Processor.Context context) {
        return new SourceArrivals(context != null && context.hazelcastInstance() != null);
    }

    void received(String stream, Envelope row) {
        if (!publishing) return;
        set("sourceRowsIn." + row.op().symbol() + "." + stream, 1);
        set("sourceBytesIn." + stream, PayloadBytes.of(row));
        if (since == null) { since = Metrics.metric("sourceCountingSince"); since.set(began); }
    }

    private void set(String name, long delta) {
        long total = totals.merge(name, delta, Long::sum);
        gauges.computeIfAbsent(name, Metrics::metric).set(total);
    }
}
