package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.metrics.JobMetrics;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.PayloadBytes;
import io.tapstate.core.lifecycle.CaptureReading;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** Per-job source-input arrivals, including replay, independent of the physical capture owner's lifetime. */
public final class SourceArrivals {
    private static final String ROWS = "sourceRowsIn.", BYTES = "sourceBytesIn.", SINCE = "sourceCountingSince";
    private final boolean publishing;
    private final long began = System.currentTimeMillis();
    private final Map<String, Long> totals = new HashMap<>();
    private final Map<String, JobStatistic> gauges = new HashMap<>();

    private SourceArrivals(boolean publishing) { this.publishing = publishing; }

    public static SourceArrivals of(Processor.Context context) {
        return new SourceArrivals(context != null && context.hazelcastInstance() != null);
    }

    /** Called once when this source takes a row from its private buffer or its own shared-log reader. */
    public void received(String stream, Envelope row) {
        if (!publishing) return;
        set(ROWS + row.op().symbol() + "." + stream, 1);
        set(BYTES + stream, PayloadBytes.of(row));
        gauges.computeIfAbsent(SINCE, JobStatistic::new).set(began);
    }

    private void set(String name, long delta) {
        long total = totals.merge(name, delta, Long::sum);
        gauges.computeIfAbsent(name, JobStatistic::new).set(total);
    }

    static CaptureReading reading(JobMetrics metrics) {
        var since = metrics.get(SINCE).stream().mapToLong(value -> value.value()).max();
        if (since.isEmpty()) return CaptureReading.NONE;
        var rows = new HashMap<String, Map<String, Long>>();
        var bytes = new HashMap<String, Long>();
        for (String metric : metrics.metrics()) {
            if (metric.startsWith(ROWS)) {
                String name = metric.substring(ROWS.length());
                int split = name.indexOf('.');
                if (split < 1 || split == name.length() - 1) continue;
                var byOp = rows.computeIfAbsent(name.substring(split + 1), ignored -> new HashMap<>());
                for (var value : metrics.get(metric)) byOp.merge(name.substring(0, split), value.value(), Long::sum);
            } else if (metric.startsWith(BYTES) && metric.length() > BYTES.length()) {
                for (var value : metrics.get(metric)) bytes.merge(metric.substring(BYTES.length()), value.value(), Long::sum);
            }
        }
        return new CaptureReading(rows, bytes, Instant.ofEpochMilli(since.getAsLong()));
    }
}
