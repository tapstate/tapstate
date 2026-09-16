package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
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
 * The two measurements a person actually asks a running pipeline for: how much it has moved, and how far
 * behind it is. They are the first that carry their dimensions as attributes rather than baked into a
 * name, so they are also the first that the flat face the command line reads cannot hold as they are —
 * and what that face does with them instead is pinned here rather than left to whoever looks next.
 *
 * <p>The honesty of the second one is the part worth the most care. What is published is the age of the
 * last row that reached a target, which is not the same as how far the source is ahead: a source with
 * nothing new to send drives it up while the pipeline is perfectly caught up. Working it out at the moment
 * somebody asks, rather than where the row settled, is what keeps a stalled pipeline from reading healthy
 * forever - and it is the reason a case here moves the clock without moving the data.
 */
class HowMuchMovedAndHowCurrentItIsReachTheReadFaceTest {

    private static final Instant STARTED = Instant.parse("2026-09-16T12:00:00Z");
    private static final Instant AT = Instant.parse("2026-09-16T12:05:00Z");

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final CapturingObservationStore observations = new CapturingObservationStore();

    /** What a two-table run reports: rows confirmed by their targets, and how recent the newest are. */
    private static DeliveryReading twoTables() {
        return new DeliveryReading(
                Map.of("orders", Map.of("i", 1_180L, "u", 20L), "items", Map.of("d", 38L)),
                Map.of("orders", AT.minusSeconds(2).toEpochMilli(),
                        "items", AT.minusSeconds(47).toEpochMilli()),
                STARTED);
    }

    @Test
    @DisplayName("rows confirmed by a target arrive as one counter carrying its dimensions")
    void theRowsThatLandedArriveAsACounterBrokenOutByTableAndOperation() {
        MetricFact records = factNamed(facts(twoTables(), AT), "tapstate.pipeline.records");

        assertThat(records.type()).isEqualTo(MetricType.COUNTER);
        assertThat(records.unit()).isEqualTo("{record}");
        assertThat(records.points()).extracting(MetricPoint::attributes)
                .containsExactlyInAnyOrder(
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders",
                                "direction", "out", "op", "insert"),
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders",
                                "direction", "out", "op", "update"),
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "items",
                                "direction", "out", "op", "delete"));
    }

    @Test
    @DisplayName("the counter says what it accumulates from, which is the run's own start")
    void theCounterCarriesTheStartTheRunReported() {
        MetricFact records = factNamed(facts(twoTables(), AT), "tapstate.pipeline.records");

        // Without this a consumer cannot tell a restart from a count going backwards, and every family
        // already on this face is a reading rather than a counter for exactly that want.
        assertThat(records.points()).allSatisfy(
                point -> assertThat(point.startTime()).isEqualTo(STARTED));
    }

    @Test
    @DisplayName("how far behind a table is, is worked out at the moment it is asked")
    void theAgeIsTakenAgainstTheClockOfWhoeverAsks() {
        MetricFact lag = factNamed(facts(twoTables(), AT), "tapstate.pipeline.lag");

        assertThat(lag.type()).isEqualTo(MetricType.GAUGE);
        assertThat(lag.unit()).isEqualTo("s");
        assertThat(lag.points()).extracting(point -> point.attributes().get("tapstate.table.id"),
                        MetricPoint::value)
                .containsExactlyInAnyOrder(tuple2("orders", 2L), tuple2("items", 47L));
    }

    @Test
    @DisplayName("a pipeline that stops moving goes on falling behind, with nothing new arriving")
    void theAgeKeepsGrowingWhileTheSameRowStaysTheNewest() {
        DeliveryReading stalled = twoTables();

        long soon = valueFor(factNamed(facts(stalled, AT), "tapstate.pipeline.lag"), "orders");
        long later = valueFor(
                factNamed(facts(stalled, AT.plusSeconds(600)), "tapstate.pipeline.lag"), "orders");

        // Identical readings from the run, ten minutes apart. An age worked out where the row settled
        // would be the same number twice, and a pipeline that died would read as healthy for as long as it
        // stayed dead.
        assertThat(soon).isEqualTo(2L);
        assertThat(later).isEqualTo(602L);
    }

    @Test
    @DisplayName("a source clock ahead of ours reads as caught up, never as a negative age")
    void anEventFromTheFutureIsNotANegativeAge() {
        DeliveryReading ahead = new DeliveryReading(Map.of("orders", Map.of("i", 1L)),
                Map.of("orders", AT.plusSeconds(90).toEpochMilli()), STARTED);

        assertThat(valueFor(factNamed(facts(ahead, AT), "tapstate.pipeline.lag"), "orders")).isZero();
    }

    @Test
    @DisplayName("a snapshot read lands under the name the contract keeps for an op it does not name")
    void aSnapshotReadIsCarriedAsTheContractsOtherRatherThanANameInventedHere() {
        DeliveryReading loading = new DeliveryReading(Map.of("orders", Map.of("r", 5_000L)),
                Map.of("orders", AT.toEpochMilli()), STARTED);

        MetricFact records = factNamed(facts(loading, AT), "tapstate.pipeline.records");

        // The engine's set of change kinds is closed at five and so is the contract's, but they are not
        // the same five. Giving it a name here would be this layer deciding a contract it implements.
        assertThat(records.points()).singleElement()
                .satisfies(point -> assertThat(point.attributes().get("op")).isEqualTo("other"));
    }

    @Test
    @DisplayName("the stored flat view carries a total per direction and an age per table")
    void theFlatFaceShowsTheThroughputCollapsedAndTheAgePerTable() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(twoTables(), AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // 1180 + 20 + 38: every table and operation added together, because a person at a command line
        // asking how fast a pipeline is going is asking about the pipeline.
        assertThat(published.metrics()).contains(
                Map.entry("records.out", 1_238L),
                Map.entry("lag.orders", 2L),
                Map.entry("lag.items", 47L));
        // The families that were already here are untouched by any of it.
        assertThat(published.metrics()).containsKeys("errorCount", "recordCount");
    }

    @Test
    @DisplayName("the flat view reports the collapse rather than performing it quietly")
    void theCollapseIsOnTheRecordAndIsNotADrop() {
        FlatMetricProjection projected = projectionOf(facts(twoTables(), AT));

        assertThat(projected.reduced())
                .containsExactlyInAnyOrder("tapstate.pipeline.records", "tapstate.pipeline.lag");
        assertThat(projected.dropped()).isEmpty();
    }

    @Test
    @DisplayName("a run reporting nothing publishes neither measurement, rather than either at zero")
    void aPipelineWithNoRunIsAbsentFromBothRatherThanPresentAtZero() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(DeliveryReading.NONE, AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // Absent, not zero. A pipeline that has delivered nothing and one whose delivery counting is not
        // wired want opposite responses, and a published zero spells them the same way.
        assertThat(published.metrics()).doesNotContainKeys("records.out", "lag.orders");
        assertThat(facts(DeliveryReading.NONE, AT)).extracting(MetricFact::name)
                .doesNotContain("tapstate.pipeline.records", "tapstate.pipeline.lag");
    }

    private List<MetricFact> facts(DeliveryReading reading, Instant at) {
        return publisherAt(reading, at).facts("orders", PipelineState.RUNNING, at,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static FlatMetricProjection projectionOf(List<MetricFact> facts) {
        return FlatMetricProjection.of(facts, Map.of(
                "tapstate.pipeline.records", attributes -> "records." + attributes.get("direction"),
                "tapstate.pipeline.lag", attributes -> "lag." + attributes.get("tapstate.table.id")));
    }

    private ObservationPublisher publisherAt(DeliveryReading reading, Instant at) {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.of(128_500L),
                id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> reading,
                Clock.fixed(at, ZoneOffset.UTC));
    }

    private static MetricFact factNamed(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no fact named " + name + " among "
                        + facts.stream().map(MetricFact::name).toList()));
    }

    private static long valueFor(MetricFact fact, String table) {
        return fact.points().stream()
                .filter(point -> table.equals(point.attributes().get("tapstate.table.id")))
                .findFirst().orElseThrow().value();
    }

    private static org.assertj.core.groups.Tuple tuple2(String table, long value) {
        return org.assertj.core.groups.Tuple.tuple(table, value);
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
