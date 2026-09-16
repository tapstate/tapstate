package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The publisher measures a set of facts and the stored observation carries a flat view of them. This
 * pins the relationship between the two: today the view carries every one, so a metric that stops
 * appearing on it stopped because somebody changed what is measured, not because the view quietly
 * had no room.
 *
 * <p>The day that stops being true is the day a metric arrives with dimensions, which no
 * {@code name -> value} map can hold. That is a decision about what the face shows, and this test is
 * what makes somebody take it: without it the first such metric is simply absent for every reader of
 * this face, and absent here is indistinguishable from never wired.
 */
class EveryMetricMeasuredReachesTheStoredViewTest {

    private static final Instant AT = Instant.parse("2026-09-16T12:00:00Z");

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final CapturingObservationStore observations = new CapturingObservationStore();

    /** A publisher with every statistic source wired, so all of the families are in play at once. */
    private ObservationPublisher everythingWired() {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.of(128_500L),
                id -> Map.of("orders", "w7"),
                id -> Map.of(),
                id -> Map.of("chain-a", 4L),
                id -> Map.of("nest.orders.doc.$root",
                        new NestStateReading(4_000L, 900L, 30L, 210L, 9_512L, OptionalLong.of(400_000L))),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of("chain-b", 61_000L),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of("nest.orders.doc.$root", 3L),
                id -> Map.of("orders.region", 120L),
                id -> Map.of("orders.region", 1_000L),
                Clock.fixed(AT, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("the stored view holds exactly the metrics that were measured")
    void theFlatViewDropsNothingThatIsMeasuredToday() {
        state.create("orders", PipelineState.RUNNING.name(), AT);
        ObservationPublisher publisher = everythingWired();

        publisher.publish("orders");

        List<MetricFact> measured = publisher.facts("orders", PipelineState.RUNNING, AT,
                Map.of("nest.orders.doc.$root",
                        new NestStateReading(4_000L, 900L, 30L, 210L, 9_512L, OptionalLong.of(400_000L))),
                Map.of("chain-a", 4L), Map.of("chain-b", 61_000L),
                Map.of("nest.orders.doc.$root", 3L), Map.of("orders.region", 120L),
                Map.of("orders.region", 1_000L));
        // Every family is actually in play, so this is not an empty set agreeing with an empty set: the
        // count is asserted before the two are compared.
        assertThat(measured).hasSize(13);
        assertThat(FlatMetricProjection.of(measured).dropped()).isEmpty();

        Observation published = observations.read("orders").orElseThrow();
        assertThat(published.metrics().keySet())
                .containsExactlyInAnyOrderElementsOf(measured.stream().map(MetricFact::name).toList());
    }

    @Test
    @DisplayName("the facts carry units the flat view has nowhere to put")
    void theUnitTravelsWithTheFactRatherThanTheName() {
        List<MetricFact> measured = everythingWired().facts("orders", PipelineState.RUNNING, AT,
                Map.of("nest.orders.doc.$root", new NestStateReading(4_000L, 900L, 30L, 210L)),
                Map.of("chain-a", 4L), Map.of("chain-b", 61_000L), Map.of(), Map.of(), Map.of());

        assertThat(measured).filteredOn(fact -> fact.name().equals("frontierStalledMillis.chain-b"))
                .singleElement()
                .satisfies(fact -> assertThat(fact.unit()).isEqualTo("ms"));
        assertThat(measured).filteredOn(fact -> fact.name().equals("recordCount"))
                .singleElement()
                .satisfies(fact -> assertThat(fact.unit()).isEqualTo("{record}"));
    }

    @Test
    @DisplayName("one pass stamps every point, and the observation, with the same instant")
    void everyPointSharesTheObservationTime() {
        state.create("orders", PipelineState.RUNNING.name(), AT);
        ObservationPublisher publisher = everythingWired();

        publisher.publish("orders");

        List<MetricFact> measured = publisher.facts("orders", PipelineState.RUNNING, AT,
                Map.of("nest.orders.doc.$root", new NestStateReading(4_000L, 900L, 30L, 210L)),
                Map.of("chain-a", 4L), Map.of("chain-b", 61_000L), Map.of(), Map.of(), Map.of());
        // A rate is a difference of values over a difference of these. Points of one pass stamped at
        // different instants would put part of this publisher's own loop into that divisor.
        assertThat(measured).isNotEmpty()
                .allSatisfy(fact -> assertThat(fact.points()).singleElement()
                        .satisfies(point -> assertThat(point.observedAt()).isEqualTo(AT)));
        assertThat(observations.read("orders").orElseThrow().observedAt()).isEqualTo(AT);
    }

    /** Keeps the latest observation written, which is what a read face would find. */
    private static final class CapturingObservationStore implements ObservationStore {

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
