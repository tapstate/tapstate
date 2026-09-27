package io.tapstate.adapters.pdk;

import io.tapstate.core.lifecycle.HistogramBounds;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class PdkExternalCallStatsTest {

    @Test
    void durationIsMeasuredOnlyWhenOneCallCompletesAndHasTheRegisteredBounds() {
        AtomicLong now = new AtomicLong(1_000_000_000L);
        PdkExternalCallStats stats = new PdkExternalCallStats(true, now::get);
        long began = stats.begin();
        now.addAndGet(25_000_000L);
        assertThat(stats.snapshot().get(PdkExternalCallStats.Call.SINK_WRITE)
                .get(PdkExternalCallStats.Outcome.SUCCESS).count()).isZero();
        stats.completed(PdkExternalCallStats.Call.SINK_WRITE, began, true);

        PdkExternalCallStats.Reading reading = stats.snapshot().get(PdkExternalCallStats.Call.SINK_WRITE)
                .get(PdkExternalCallStats.Outcome.SUCCESS);
        assertThat(reading.count()).isEqualTo(1);
        assertThat(reading.duration().count()).isEqualTo(1);
        assertThat(reading.duration().sum()).isEqualTo(0.025);
        assertThat(reading.duration().bounds())
                .isEqualTo(HistogramBounds.CONNECTOR_EXTERNAL_CALL_DURATION.bounds());
        assertThat(reading.duration().bucketCounts()).containsExactly(
                0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    @Test
    void disabledRecorderProducesNoProcessReading() {
        PdkExternalCallStats stats = PdkExternalCallStats.disabled();
        long began = stats.begin();
        stats.completed(PdkExternalCallStats.Call.SNAPSHOT_READ, began, false);
        assertThat(stats.snapshot()).isEmpty();
    }
}
