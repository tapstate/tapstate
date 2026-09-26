package io.tapstate.runtime.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SinkBatchReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.StageReading;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.core.lifecycle.Observation;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** Sink batch facts are measured on a live writer path and never filled with unwired zeros. */
class SinkBatchFactsTest {

    private static final Instant AT = Instant.parse("2026-09-27T12:00:00Z");
    private static final Instant SINCE = Instant.parse("2026-09-27T11:00:00Z");
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final CapturingStore observations = new CapturingStore();

    private static HistogramValue duration(HistogramBounds bounds, long count, double sum) {
        List<Long> buckets = new ArrayList<>();
        for (int index = 0; index < bounds.buckets(); index++) {
            buckets.add(index == 7 ? count : 0L);
        }
        return bounds.value(count, sum, buckets);
    }

    private ObservationPublisher publisher(SinkBatchReading batch) {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> SnapshotReading.NONE,
                id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> CaptureReading.NONE, id -> DeliveryReading.NONE, id -> StageReading.NONE,
                id -> batch, Clock.fixed(AT, ZoneOffset.UTC));
    }

    private List<MetricFact> facts(SinkBatchReading batch) {
        return publisher(batch).facts("orders", PipelineState.RUNNING, AT,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), SnapshotReading.NONE);
    }

    @Test
    void quiet_or_unwired_sink_has_no_batch_series() {
        assertThat(facts(SinkBatchReading.NONE)).extracting(MetricFact::name)
                .noneMatch(name -> name.startsWith("tapstate.pipeline.sink."));
    }

    @Test
    void measured_batch_facts_reach_the_stored_face_with_units_and_a_closed_pipeline_dimension() {
        SinkBatchReading batch = new SinkBatchReading(3, 7, 4, 0, 1, 0L,
                duration(HistogramBounds.SINK_BATCH_WRITE_DURATION, 3, 0.4),
                duration(HistogramBounds.SINK_BACKPRESSURE_DURATION, 1, 0.1), SINCE);
        state.create("orders", PipelineState.RUNNING.name(), AT);
        publisher(batch).publish("orders");
        List<MetricFact> measured = observations.read("orders").orElseThrow().facts();

        assertThat(measured).extracting(MetricFact::name).contains(
                "tapstate.pipeline.sink.batch.issued", "tapstate.pipeline.sink.batch.records",
                "tapstate.pipeline.sink.batch.records.max", "tapstate.pipeline.sink.batch.pending",
                "tapstate.pipeline.sink.batch.limit", "tapstate.pipeline.sink.backpressured",
                "tapstate.pipeline.sink.batch.write.duration", "tapstate.pipeline.sink.backpressure.duration");
        assertThat(measured.stream().filter(fact -> fact.name().startsWith("tapstate.pipeline.sink.")))
                .allSatisfy(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
                    assertThat(point.attributes()).isEqualTo(Map.of("tapstate.pipeline.id", "orders"));
                    assertThat(point.observedAt()).isEqualTo(AT);
                }));
        MetricFact records = named(measured, "tapstate.pipeline.sink.batch.records");
        assertThat(records.type()).isEqualTo(MetricType.COUNTER);
        assertThat(records.unit()).isEqualTo("{record}");
        assertThat(records.points().getFirst().startTime()).isEqualTo(SINCE);
        assertThat(records.points().getFirst().value()).isEqualTo(7L);
        MetricFact pending = named(measured, "tapstate.pipeline.sink.batch.pending");
        assertThat(pending.type()).isEqualTo(MetricType.GAUGE);
        assertThat(pending.points().getFirst().value()).isZero();
        assertThat(named(measured, "tapstate.pipeline.sink.batch.write.duration").points().getFirst()
                .histogram().sum()).isEqualTo(0.4);
        assertThat(FlatMetricProjection.of(measured, ObservationPublisher.FLAT_REDUCTIONS).dropped())
                .contains("tapstate.pipeline.sink.batch.write.duration");
    }

    private static MetricFact named(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name)).findFirst().orElseThrow();
    }

    private static final class CapturingStore implements ObservationStore {
        private final Map<String, Observation> saved = new HashMap<>();

        @Override public void save(Observation observation) {
            saved.put(observation.pipelineId(), observation);
        }

        @Override public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(saved.get(pipelineId));
        }

        @Override public void delete(String pipelineId) {
            saved.remove(pipelineId);
        }
    }
}
