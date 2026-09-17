package io.tapstate.runtime.scheduler;

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
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.StageReading;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where a run spends its time reaches the facts as one distribution per stage, and reaches the flat face
 * not at all — a distribution has no single number to be, and the face says so rather than squeezing it.
 */
class WhereTheRunSpendsItsTimeReachesTheReadFaceTest {

    private static final Instant AT = Instant.parse("2026-09-17T12:00:00Z");
    private static final Instant STARTED = Instant.parse("2026-09-17T11:00:00Z");

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final CapturingStore observations = new CapturingStore();

    private static HistogramValue units(long count, double sumSeconds, int bucket) {
        List<Long> buckets = new ArrayList<>();
        for (int index = 0; index < HistogramBounds.PROCESS_DURATION.buckets(); index++) {
            buckets.add(index == bucket ? count : 0L);
        }
        return HistogramBounds.PROCESS_DURATION.value(count, sumSeconds, buckets);
    }

    private static StageReading twoStages() {
        return new StageReading(Map.of(
                "transform", units(1_000L, 0.4, 2),
                "sink", units(12L, 0.9, 9)), STARTED);
    }

    private ObservationPublisher publisherWith(StageReading spent) {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(),
                id -> Map.of(), id -> SnapshotReading.NONE, id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> CaptureReading.NONE,
                id -> DeliveryReading.NONE,
                id -> spent,
                Clock.fixed(AT, ZoneOffset.UTC));
    }

    private List<MetricFact> facts(StageReading spent) {
        return publisherWith(spent).facts("orders", PipelineState.RUNNING, AT,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), SnapshotReading.NONE);
    }

    @Test
    @DisplayName("each stage's distribution is a point of one histogram fact, keyed by the stage attribute")
    void eachStageIsAPointOfOneHistogramFact() {
        List<MetricFact> facts = facts(twoStages());

        MetricFact spent = facts.stream().filter(fact -> fact.name().equals("tapstate.pipeline.process.duration"))
                .findFirst().orElseThrow();
        assertThat(spent.type()).isEqualTo(MetricType.HISTOGRAM);
        assertThat(spent.unit()).isEqualTo("s");
        assertThat(spent.points()).extracting(MetricPoint::attributes).containsExactlyInAnyOrder(
                Map.of("tapstate.pipeline.id", "orders", "stage", "transform"),
                Map.of("tapstate.pipeline.id", "orders", "stage", "sink"));
        assertThat(spent.points()).allSatisfy(point -> {
            assertThat(point.startTime()).isEqualTo(STARTED);
            assertThat(point.observedAt()).isEqualTo(AT);
        });
        assertThat(spent.points()).filteredOn(point -> "sink".equals(point.attributes().get("stage")))
                .singleElement().satisfies(point -> assertThat(point.histogram()).isEqualTo(units(12L, 0.9, 9)));
    }

    @Test
    @DisplayName("a run that has timed nothing publishes no such fact rather than five stages at zero")
    void aRunThatTimedNothingPublishesNoFact() {
        assertThat(facts(StageReading.NONE)).extracting(MetricFact::name)
                .doesNotContain("tapstate.pipeline.process.duration");
    }

    @Test
    @DisplayName("the flat face cannot carry it and says so")
    void theFlatFaceDropsItAndSaysSo() {
        FlatMetricProjection flat = FlatMetricProjection.of(facts(twoStages()), ObservationPublisher.FLAT_REDUCTIONS);

        assertThat(flat.dropped()).contains("tapstate.pipeline.process.duration");
        assertThat(flat.metrics().keySet()).noneMatch(key -> key.contains("process.duration"));
    }

    @Test
    @DisplayName("the stored observation carries the fact whole beside the flat map")
    void theStoredObservationCarriesTheFactWhole() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherWith(twoStages()).publish("orders");

        assertThat(observations.read("orders").orElseThrow().facts())
                .extracting(MetricFact::name).contains("tapstate.pipeline.process.duration");
    }

    /** Keeps the latest observation written, which is what a read face would find. */
    private static final class CapturingStore implements ObservationStore {

        private final Map<String, Observation> saved = new HashMap<>();

        @Override
        public void save(Observation observation) {
            saved.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(saved.get(pipelineId));
        }

        @Override
        public void delete(String pipelineId) {
            saved.remove(pipelineId);
        }
    }
}
