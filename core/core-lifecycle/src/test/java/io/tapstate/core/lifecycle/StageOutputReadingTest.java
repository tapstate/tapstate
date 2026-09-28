package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageOutputReadingTest {
    private static final Instant START = Instant.parse("2026-09-29T08:00:00Z");
    private static final Instant AT = START.plusSeconds(1);

    @Test
    void oldRuntimePortConstructorKeepsTheOutputExtensionAbsent() {
        var old = new StageRuntimeReading(new StageWorkReading(Map.of("transform", 1L), AT), StageQueueReading.NONE);
        assertThat(old.output()).isEqualTo(StageOutputReading.NONE);
        assertThat(old.activeWork().totalActive()).isEqualTo(1);
    }

    @Test
    void quietSinkAndUnknownStagesCannotBecomeOutputPressureReadings() {
        assertThatThrownBy(() -> new StageOutputReading.Sample(0, null, START, AT))
                .isInstanceOf(IllegalArgumentException.class);
        for (String stage : java.util.List.of("sink", "executor")) {
            assertThatThrownBy(() -> new StageOutputReading(Map.of(stage,
                    new StageOutputReading.Sample(1, null, START, AT))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void aRetryReadingCannotCarryMoreCompletedIntervalsThanRefusalsOrMismatchedBuckets() {
        var buckets = new ArrayList<>(Collections.nCopies(HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.buckets(), 0L));
        buckets.set(0, 2L);
        var two = HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.value(2, 0.0002, buckets);
        assertThatThrownBy(() -> new StageOutputReading.Sample(1, two, START, AT))
                .isInstanceOf(IllegalArgumentException.class);
        buckets.set(0, 1L);
        var mismatched = HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.value(2, 0.0002, buckets);
        assertThatThrownBy(() -> new StageOutputReading.Sample(3, mismatched, START, AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StageOutputReading.Sample(1, null, AT, START))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
