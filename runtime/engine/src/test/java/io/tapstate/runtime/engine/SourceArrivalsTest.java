package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import io.tapstate.core.lifecycle.CaptureReading;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SourceArrivalsTest {
    @Test void sumsOnlySourceInputsAndRetainsQualifiedTablesAndTheLatestCounterStart() {
        var input = new LinkedHashMap<String, List<Measurement>>();
        Map.of("sourceRowsIn.i.orders", List.of(2L, 3L), "sourceRowsIn.r.public.items", List.of(1L),
                "sourceBytesIn.orders", List.of(20L, 30L), "sourceCountingSince", List.of(1000L, 2000L),
                "recordsOut.i.orders", List.of(99L), "bytesOut.orders", List.of(900L))
                .forEach((key, values) -> input.put(key, values.stream().map(value -> Measurement.of(key, value, 0, Map.of())).toList()));
        var reading = SourceArrivals.reading(JobMetrics.of(input));
        assertThat(reading.rowsByTableAndOp()).containsExactlyInAnyOrderEntriesOf(
                Map.of("orders", Map.of("i", 5L), "public.items", Map.of("r", 1L)));
        assertThat(reading.bytesByTable()).containsOnly(Map.entry("orders", 50L));
        assertThat(reading.countingSince()).isEqualTo(Instant.ofEpochMilli(2000));
    }

    @Test void absentCounterOriginIsNotAMeasuredZero() {
        assertThat(SourceArrivals.reading(JobMetrics.empty())).isEqualTo(CaptureReading.NONE);
        var unstamped = JobMetrics.of(Map.of("sourceRowsIn.i.orders",
                List.of(Measurement.of("sourceRowsIn.i.orders", 3, 0, Map.of()))));
        assertThat(SourceArrivals.reading(unstamped)).isEqualTo(CaptureReading.NONE);
    }
}
