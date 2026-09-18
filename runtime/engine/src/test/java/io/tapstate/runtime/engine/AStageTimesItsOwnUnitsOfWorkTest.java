package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stage timer counts each unit of work into the registered buckets and reports the distribution so far
 * every time, against a clock it is given rather than the wall — so a unit of a known length can be
 * witnessed to land in a known bucket.
 */
class AStageTimesItsOwnUnitsOfWorkTest {

    /** A nanosecond clock that advances only when told to. */
    private static final class FakeNanos {
        private long now = 1_000_000_000L;

        long read() {
            return now;
        }

        void advanceMicros(long micros) {
            now += micros * 1_000L;
        }
    }

    /** Keeps every reading handed over, so what was reported and how often can both be asserted. */
    private static final class RecordingStageGauge implements StageGauge {
        private final List<Stage> stages = new ArrayList<>();
        private final List<HistogramValue> distributions = new ArrayList<>();
        private final List<Long> starts = new ArrayList<>();

        @Override
        public void took(Stage stage, long count, long sumNanos, long[] bucketCounts, long countingSinceMillis) {
            stages.add(stage);
            // Copied, not kept: the array handed over is the timer's own and goes on changing. A double
            // that kept it would show every reading as the last one, and a reading asserted against the
            // final state of the run is a reading nobody checked.
            List<Long> counts = new ArrayList<>(bucketCounts.length);
            for (long bucket : bucketCounts) {
                counts.add(bucket);
            }
            distributions.add(HistogramBounds.PROCESS_DURATION.value(count, sumNanos / 1_000_000_000.0, counts));
            starts.add(countingSinceMillis);
        }

        HistogramValue latest() {
            return distributions.get(distributions.size() - 1);
        }
    }

    @Test
    @DisplayName("each unit is counted into the bucket its length falls in, and the sum is in seconds")
    void eachUnitLandsInTheBucketItsLengthFallsIn() {
        FakeNanos clock = new FakeNanos();
        RecordingStageGauge gauge = new RecordingStageGauge();
        StageTimer timer = new StageTimer(Stage.TRANSFORM, gauge, clock::read, 1_700_000_000_000L);

        long first = timer.begin();
        clock.advanceMicros(300);
        timer.end(first);
        long second = timer.begin();
        clock.advanceMicros(40_000);
        timer.end(second);

        HistogramValue took = gauge.latest();
        assertThat(took.count()).isEqualTo(2L);
        assertThat(took.sum()).isEqualTo(0.0403);
        assertThat(took.bounds()).isEqualTo(HistogramBounds.PROCESS_DURATION.bounds());
        // 300 us is under the 500 us bound (index 2); 40 ms is under the 50 ms bound (index 8).
        assertThat(took.bucketCounts().get(2)).isEqualTo(1L);
        assertThat(took.bucketCounts().get(8)).isEqualTo(1L);
        assertThat(took.bucketCounts().stream().mapToLong(Long::longValue).sum()).isEqualTo(2L);
    }

    @Test
    @DisplayName("the distribution is reported after every unit, for the stage it belongs to, with its start")
    void theDistributionIsReportedAfterEveryUnitWithItsStageAndStart() {
        FakeNanos clock = new FakeNanos();
        RecordingStageGauge gauge = new RecordingStageGauge();
        StageTimer timer = new StageTimer(Stage.NEST, gauge, clock::read, 1_700_000_000_000L);

        for (int unit = 0; unit < 3; unit++) {
            long started = timer.begin();
            clock.advanceMicros(10);
            timer.end(started);
        }

        assertThat(gauge.distributions).hasSize(3);
        assertThat(gauge.stages).containsOnly(Stage.NEST);
        assertThat(gauge.starts).containsOnly(1_700_000_000_000L);
        assertThat(gauge.latest().count()).isEqualTo(3L);
    }

    @Test
    @DisplayName("a unit exactly on a bound lands in the bucket that bound closes")
    void aUnitExactlyOnABoundLandsInTheBucketThatBoundCloses() {
        FakeNanos clock = new FakeNanos();
        RecordingStageGauge gauge = new RecordingStageGauge();
        StageTimer timer = new StageTimer(Stage.JOIN, gauge, clock::read, 0L);

        long started = timer.begin();
        clock.advanceMicros(1_000); // exactly 1 ms, the bound at index 3
        timer.end(started);

        assertThat(gauge.latest().bucketCounts().get(3)).isEqualTo(1L);
        assertThat(gauge.latest().bucketCounts().get(4)).isEqualTo(0L);
    }

    @Test
    @DisplayName("a unit longer than the last bound lands in the overflow bucket")
    void aUnitPastTheLastBoundLandsInTheOverflowBucket() {
        FakeNanos clock = new FakeNanos();
        RecordingStageGauge gauge = new RecordingStageGauge();
        StageTimer timer = new StageTimer(Stage.SINK, gauge, clock::read, 0L);

        long started = timer.begin();
        clock.advanceMicros(25_000_000); // 25 s, past the 10 s bound
        timer.end(started);

        assertThat(gauge.latest().bucketCounts().get(HistogramBounds.PROCESS_DURATION.buckets() - 1)).isEqualTo(1L);
    }

    @Test
    @DisplayName("outside a job the timer still counts, and reports to nobody")
    void outsideAJobTheTimerCountsForNobody() {
        StageTimer timer = StageTimer.of(Stage.SOURCE, null);

        long started = timer.begin();
        timer.end(started);

        assertThat(timer.value().count()).isEqualTo(1L);
        assertThat(timer.stage()).isEqualTo(Stage.SOURCE);
    }

    @Test
    @DisplayName("the statistics' names split back into the stage and the part they carry")
    void theNamesSplitBackIntoStageAndPart() {
        assertThat(JetStageGauge.partOf("stage.transform.count"))
                .isEqualTo(new JetStageGauge.Part("transform", ".count", -1));
        assertThat(JetStageGauge.partOf("stage.nest.sumMicros"))
                .isEqualTo(new JetStageGauge.Part("nest", ".sumMicros", -1));
        assertThat(JetStageGauge.partOf("stage.sink.bucket.16"))
                .isEqualTo(new JetStageGauge.Part("sink", ".bucket.", 16));
        assertThat(JetStageGauge.partOf("stage.source.since"))
                .isEqualTo(new JetStageGauge.Part("source", ".since", -1));
        assertThat(JetStageGauge.partOf("stage.join.bucket.x")).isNull();
        assertThat(JetStageGauge.partOf("stage.join")).isNull();
        assertThat(JetStageGauge.partOf("recordsOut.i.orders")).isNull();
    }
}
