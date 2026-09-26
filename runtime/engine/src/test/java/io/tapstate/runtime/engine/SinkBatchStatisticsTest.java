package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.SinkBatchReading;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The reader combines fixed sink statistics across processors and rejects missing parts. */
class SinkBatchStatisticsTest {

    private static JobMetrics collected(Map<String, List<Long>> values) {
        Map<String, List<Measurement>> measurements = new LinkedHashMap<>();
        values.forEach((name, readings) -> measurements.put(name,
                readings.stream().map(value -> Measurement.of(name, value, 0L, Map.of())).toList()));
        return JobMetrics.of(measurements);
    }

    @Test
    void absent_or_half_collected_batch_does_not_claim_a_zero_measurement() {
        assertThat(Engine.sinkBatchReadingIn(collected(Map.of()))).isEqualTo(SinkBatchReading.NONE);
        assertThat(Engine.sinkBatchReadingIn(collected(Map.of(
                JetSinkUseGauge.ISSUED, List.of(1L))))).isEqualTo(SinkBatchReading.NONE);
    }

    @Test
    void sums_work_and_pending_across_sinks_but_keeps_the_largest_batch_and_latest_start() {
        Map<String, List<Long>> parts = new LinkedHashMap<>();
        parts.put(JetSinkUseGauge.ISSUED, List.of(2L, 3L));
        parts.put(JetSinkUseGauge.RECORDS, List.of(4L, 9L));
        parts.put(JetSinkUseGauge.LARGEST, List.of(2L, 3L));
        parts.put(JetSinkUseGauge.PENDING, List.of(1L, 0L));
        parts.put(JetSinkUseGauge.LIMIT, List.of(1L, 1L));
        parts.put(JetSinkUseGauge.BACKPRESSURED, List.of(1L, 0L));
        parts.put(JetSinkUseGauge.SINCE, List.of(1_700_000_000_000L, 1_700_000_005_000L));
        parts.put(JetSinkUseGauge.WRITE + JetSinkUseGauge.COUNT, List.of(1L, 2L));
        parts.put(JetSinkUseGauge.WRITE + JetSinkUseGauge.SUM_MICROS, List.of(100_000L, 300_000L));
        for (int index = 0; index < HistogramBounds.SINK_BATCH_WRITE_DURATION.buckets(); index++) {
            parts.put(JetSinkUseGauge.WRITE + JetSinkUseGauge.BUCKET + index,
                    index == 7 ? List.of(1L, 2L) : List.of(0L, 0L));
        }

        SinkBatchReading read = Engine.sinkBatchReadingIn(collected(parts));

        assertThat(read.issuedBatches()).isEqualTo(5L);
        assertThat(read.issuedRecords()).isEqualTo(13L);
        assertThat(read.largestBatch()).isEqualTo(3L);
        assertThat(read.pendingBatches()).isEqualTo(1L);
        assertThat(read.inFlightLimit()).isEqualTo(2L);
        assertThat(read.backpressuredSinks()).isEqualTo(1L);
        assertThat(read.countingSince()).isEqualTo(Instant.ofEpochMilli(1_700_000_005_000L));
        assertThat(read.writeDuration().count()).isEqualTo(3L);
        assertThat(read.writeDuration().sum()).isEqualTo(0.4);
        assertThat(read.writeDuration().bucketCounts().get(7)).isEqualTo(3L);
        assertThat(read.backpressureDuration()).isNull();
    }

    @Test
    void a_partial_distribution_is_absent_while_other_measured_batch_facts_remain() {
        Map<String, List<Long>> parts = new LinkedHashMap<>();
        parts.put(JetSinkUseGauge.ISSUED, List.of(1L));
        parts.put(JetSinkUseGauge.RECORDS, List.of(2L));
        parts.put(JetSinkUseGauge.LARGEST, List.of(2L));
        parts.put(JetSinkUseGauge.PENDING, List.of(1L));
        parts.put(JetSinkUseGauge.LIMIT, List.of(1L));
        parts.put(JetSinkUseGauge.SINCE, List.of(1_700_000_000_000L));
        parts.put(JetSinkUseGauge.WRITE + JetSinkUseGauge.COUNT, List.of(1L));
        parts.put(JetSinkUseGauge.WRITE + JetSinkUseGauge.SUM_MICROS, List.of(100_000L));

        SinkBatchReading read = Engine.sinkBatchReadingIn(collected(parts));

        assertThat(read.isEmpty()).isFalse();
        assertThat(read.writeDuration()).isNull();
        assertThat(read.backpressuredSinks()).isNull();
        assertThat(read.backpressureDuration()).isNull();
    }
}
