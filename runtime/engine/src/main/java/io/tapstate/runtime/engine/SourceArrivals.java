package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import io.tapstate.core.lifecycle.CaptureReading;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** Reads only per-job source-input statistics, independent of a physical capture owner's lifetime. */
final class SourceArrivals {
    private static final String ROWS = "sourceRowsIn.", BYTES = "sourceBytesIn.", SINCE = "sourceCountingSince";
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
