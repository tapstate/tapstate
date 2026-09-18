package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sampler is offered every observation and keeps one per interval, taken off exactly what was
 * published: the counters and the per-table delay a line is drawn from, and what the counters accumulate
 * from. What it does not do is as much the contract: it takes no sample from an observation with nothing
 * to draw a line from, and it never removes anything.
 */
class RateSamplerTest {

    private static final Instant T0 = Instant.parse("2026-09-17T10:00:00Z");
    private static final Instant STARTED = Instant.parse("2026-09-17T02:00:00Z");

    /** Appends only, like the real store; keeps what it was handed so a case can read it back. */
    private static final class RecordingHistory implements RateHistoryStore {
        private final List<RateSample> appended = new ArrayList<>();

        @Override
        public void append(RateSample sample) {
            appended.add(sample);
        }

        @Override
        public List<RateSample> readBetween(String pipelineId, Instant from, Instant to) {
            return appended.stream().filter(sample -> sample.pipelineId().equals(pipelineId)).toList();
        }

        @Override
        public void deleteAll(String pipelineId) {
            throw new AssertionError("the sampler never removes anything");
        }

        @Override
        public Duration retention() {
            return Duration.ofDays(15);
        }
    }

    private static Observation moving(Instant at, long out) {
        MetricFact records = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", List.of(
                MetricPoint.accumulated(Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders",
                        "direction", "out", "op", "insert"), STARTED, at, out)));
        return new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", out, "records.in", out + 10, "bytes.out", out * 100, "errors.sink.write-rejected", 1L,
                        "recordCount", out, "lag.orders", 4L, "lag.items", 47L,
                        "frontierGap.chain-a", 0L, "nestStateEntries.nest.orders.doc.$root", 12L),
                Map.of(), Map.of(), null, at, List.of(records));
    }

    @Test
    @DisplayName("one sample per interval, however many observations are offered")
    void oneSamplePerInterval() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));

        sampler.offer(moving(T0, 100L));
        sampler.offer(moving(T0.plusSeconds(1), 101L));
        sampler.offer(moving(T0.plusSeconds(59), 159L));
        sampler.offer(moving(T0.plusSeconds(60), 160L));
        sampler.offer(moving(T0.plusSeconds(61), 161L));

        assertThat(history.appended).extracting(RateSample::observedAt)
                .containsExactly(T0, T0.plusSeconds(60));
        assertThat(history.appended).extracting(sample -> sample.counters().get("records.out"))
                .containsExactly(100L, 160L);
    }

    @Test
    @DisplayName("a sample is the line-bearing subset of the flat map, at the observation's own time")
    void aSampleIsTheLineBearingSubset() {
        RecordingHistory history = new RecordingHistory();
        new RateSampler(history, Duration.ofSeconds(60)).offer(moving(T0, 100L));

        RateSample sample = history.appended.get(0);
        assertThat(sample.pipelineId()).isEqualTo("orders");
        assertThat(sample.observedAt()).isEqualTo(T0);
        assertThat(sample.counters()).containsOnlyKeys("records.out", "records.in", "bytes.out",
                "errors.sink.write-rejected", "recordCount");
        // The delay keeps its table; the diagnostic families do not travel, since no line is drawn from them.
        assertThat(sample.lag()).containsOnly(Map.entry("orders", 4L), Map.entry("items", 47L));
        // What the counters accumulate from, read off the facts beside the flat map.
        assertThat(sample.countingSince()).isEqualTo(STARTED);
    }

    @Test
    @DisplayName("an observation with nothing to draw a line from is not sampled")
    void nothingToDrawFromIsNotSampled() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));

        // A stopped pipeline publishes a state and no counters: a history of empty documents, one a
        // minute for fifteen days, would be what a stop costs.
        sampler.offer(new Observation("orders", PipelineState.STOPPED, Map.of(), Map.of(), Map.of(), null, T0));
        sampler.offer(new Observation("orders", PipelineState.RUNNING,
                Map.of("frontierGap.chain-a", 0L), Map.of(), Map.of(), null, T0.plusSeconds(60)));

        assertThat(history.appended).isEmpty();
    }

    @Test
    @DisplayName("an observation that does not say when it was taken is not sampled")
    void anObservationWithoutATimeIsNotSampled() {
        RecordingHistory history = new RecordingHistory();
        new RateSampler(history, Duration.ofSeconds(60))
                .offer(new Observation("orders", PipelineState.RUNNING, Map.of("records.out", 1L), Map.of()));

        assertThat(history.appended).isEmpty();
    }

    @Test
    @DisplayName("the cadence is per pipeline")
    void theCadenceIsPerPipeline() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));

        sampler.offer(moving(T0, 100L));
        Observation other = new Observation("items", PipelineState.RUNNING, Map.of("records.out", 5L),
                Map.of(), Map.of(), null, T0.plusSeconds(1));
        sampler.offer(other);

        assertThat(history.appended).extracting(RateSample::pipelineId).containsExactly("orders", "items");
    }

    @Test
    @DisplayName("a pipeline forgotten starts its cadence afresh, and nothing of its history is touched")
    void aForgottenPipelineStartsAfresh() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));

        sampler.offer(moving(T0, 100L));
        sampler.forgetPipelinesOutside(List.of());
        sampler.offer(moving(T0.plusSeconds(1), 101L));

        assertThat(history.appended).hasSize(2);
    }
}
