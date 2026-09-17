package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounded initial load as a monitoring backend sees it: how many rows of each table it read, and about
 * how many each of those tables was last counted to hold.
 *
 * <p>The pair are deliberately of two different kinds. What the load has read only ever rises, so it is a
 * counter and carries the moment it began — a pipeline restarted onto a fresh load would otherwise be
 * indistinguishable from one whose count went backwards. About how many rows there are does not rise
 * monotonically at all: it is what the last discovery of the source counted, nothing maintains it
 * afterwards, and the next discovery of a table that shrank will revise it downwards. Published as a
 * counter it would promise a reader something the first re-discovery breaks, quietly.
 *
 * <p>The total is left out per table and never defaulted. A pipeline can read one table off a connector
 * that can count and another off one that cannot, and a total standing in for the second would size an
 * unmeasured table at whatever stood in — which, if the stand-in were the rows already done, would read as
 * a finished load.
 */
class HowFarTheLoadGotReachesTheReadFaceTest {

    private static final Instant LOAD_BEGAN = Instant.parse("2026-09-17T11:00:00Z");
    private static final Instant AT = Instant.parse("2026-09-17T12:00:00Z");

    private static final String ROWS = "tapstate.pipeline.snapshot.rows";
    private static final String TOTAL = "tapstate.pipeline.snapshot.rows.total";

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final ObservationStore observations = new UnusedObservations();

    @Test
    @DisplayName("rows the load read arrive as a counter carrying the table they came from")
    void theRowsLoadedArriveAsACounterBrokenOutByTable() {
        MetricFact rows = factNamed(facts(oneTableCounted()), ROWS);

        assertThat(rows.type()).isEqualTo(MetricType.COUNTER);
        assertThat(rows.unit()).isEqualTo("{row}");
        assertThat(rows.points()).singleElement().satisfies(point -> {
            assertThat(point.value()).isEqualTo(90_000L);
            assertThat(point.attributes()).containsOnly(
                    Map.entry("tapstate.pipeline.id", "orders"),
                    Map.entry("tapstate.table.id", "orders"));
        });
    }

    @Test
    @DisplayName("the counter says when the load began, so a fresh load is not a count going backwards")
    void theCounterCarriesTheMomentTheLoadBegan() {
        MetricFact rows = factNamed(facts(oneTableCounted()), ROWS);

        assertThat(rows.points()).singleElement()
                .satisfies(point -> assertThat(point.startTime()).isEqualTo(LOAD_BEGAN));
    }

    @Test
    @DisplayName("about how many rows there are is a gauge, because a later discovery may revise it down")
    void theTotalArrivesAsAGaugeAndNotAsACounter() {
        MetricFact total = factNamed(facts(oneTableCounted()), TOTAL);

        assertThat(total.type()).isEqualTo(MetricType.GAUGE);
        assertThat(total.unit()).isEqualTo("{row}");
        assertThat(total.points()).singleElement()
                .satisfies(point -> assertThat(point.value()).isEqualTo(120_000L));
    }

    @Test
    @DisplayName("a table nothing could count carries no total, while the one beside it keeps its own")
    void aTableWithNoCountIsLeftOutOfTheTotalWhileItsNeighbourKeepsIt() {
        SnapshotReading mixed = new SnapshotReading(Map.of(
                "orders", new TableSnapshot(90_000L, 120_000L, 75),
                "items", new TableSnapshot(4_000L, null, null)), LOAD_BEGAN);

        List<MetricFact> measured = facts(mixed);

        // Both tables read rows, so both are counted ...
        assertThat(factNamed(measured, ROWS).points())
                .extracting(point -> point.attributes().get("tapstate.table.id"))
                .containsExactlyInAnyOrder("orders", "items");
        // ... and only the one somebody counted has a total. Neither a zero nor the rows already done
        // stands in for the other, because both of those read as a load that has finished.
        assertThat(factNamed(measured, TOTAL).points())
                .extracting(point -> point.attributes().get("tapstate.table.id"))
                .containsExactly("orders");
    }

    @Test
    @DisplayName("a pipeline that ran no load publishes neither measurement rather than a pair of zeroes")
    void noLoadPublishesNothingAtAll() {
        List<MetricFact> measured = facts(SnapshotReading.NONE);

        // Nothing else is measured in this fixture, so "neither measurement" is "no facts at all"; an
        // absence asserted over an empty list would hold on every implementation, this does not.
        assertThat(measured).isEmpty();
    }

    @Test
    @DisplayName("a load whose tables all went uncounted still publishes what it read")
    void aLoadWithNoTotalsAnywhereStillCountsItsRows() {
        SnapshotReading uncounted = new SnapshotReading(
                Map.of("orders", new TableSnapshot(500L, null, null)), LOAD_BEGAN);

        List<MetricFact> measured = facts(uncounted);

        assertThat(factNamed(measured, ROWS).points()).singleElement()
                .satisfies(point -> assertThat(point.value()).isEqualTo(500L));
        assertThat(measured).extracting(MetricFact::name).doesNotContain(TOTAL);
    }

    /** One table's finished load, counted by a connector that can count it. */
    private static SnapshotReading oneTableCounted() {
        return new SnapshotReading(Map.of("orders", new TableSnapshot(90_000L, 120_000L, 75)), LOAD_BEGAN);
    }

    private List<MetricFact> facts(SnapshotReading loaded) {
        return publisher(loaded).facts("orders", PipelineState.RUNNING, AT,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), loaded);
    }

    private ObservationPublisher publisher(SnapshotReading loaded) {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.empty(), id -> Map.of(), id -> loaded);
    }

    /**
     * A store nothing here is meant to reach. Every case below asks the publisher for its facts directly
     * rather than publishing, so a write arriving here means a case stopped measuring what it claims to:
     * it says so instead of quietly recording it.
     */
    private static final class UnusedObservations implements ObservationStore {

        @Override
        public void save(Observation observation) {
            throw new AssertionError("no case here publishes; it reads the facts directly");
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            throw new AssertionError("no case here publishes; it reads the facts directly");
        }

        @Override
        public void delete(String pipelineId) {
            throw new AssertionError("no case here publishes; it reads the facts directly");
        }
    }

    private static MetricFact factNamed(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no fact named " + name + " among " + facts.stream().map(MetricFact::name).toList()));
    }
}
