package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.TableSnapshot;
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
import static org.assertj.core.api.Assertions.entry;

/**
 * The publisher measures a set of facts and the stored observation carries them twice: whole, and as a
 * flat view. This pins the relationship between the three. The facts on the document are the facts that
 * were measured, every one of them; the flat view carries what it can and says which it could not; and a
 * metric that stops appearing on the flat view stopped because somebody changed what is measured or what
 * that view shows, not because the view quietly had no room.
 *
 * <p>A metric with dimensions or a distribution is what no {@code name -> value} map can hold. That is a
 * decision about what the flat face shows, and this test is what makes somebody take it: without it the
 * first such metric is simply absent from that face for every reader, and absent there is
 * indistinguishable from never wired. The facts beside it are where such a metric is read whole.
 */
class EveryMetricMeasuredReachesTheStoredViewTest {

    private static final Instant AT = Instant.parse("2026-09-16T12:00:00Z");
    private static final Instant LOAD_BEGAN = Instant.parse("2026-09-16T11:00:00Z");

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final CapturingObservationStore observations = new CapturingObservationStore();

    /** A publisher with every statistic source wired, so all of the families are in play at once. */
    private ObservationPublisher everythingWired() {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.of(128_500L),
                id -> Map.of("orders", "w7"),
                id -> loaded(),
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

    /** One table's finished load, with a total, so both of the load's measurements are in play. */
    private static SnapshotReading loaded() {
        return new SnapshotReading(Map.of("orders", new TableSnapshot(90_000L, 120_000L, 75)), LOAD_BEGAN);
    }

    @Test
    @DisplayName("the stored view holds every metric it can carry, and says which face carries the rest")
    void theFlatViewCarriesAllButTheLoadAndSaysSo() {
        state.create("orders", PipelineState.RUNNING.name(), AT);
        ObservationPublisher publisher = everythingWired();

        publisher.publish("orders");

        List<MetricFact> measured = publisher.facts("orders", PipelineState.RUNNING, AT,
                Map.of("nest.orders.doc.$root",
                        new NestStateReading(4_000L, 900L, 30L, 210L, 9_512L, OptionalLong.of(400_000L))),
                Map.of("chain-a", 4L), Map.of("chain-b", 61_000L),
                Map.of("nest.orders.doc.$root", 3L), Map.of("orders.region", 120L),
                Map.of("orders.region", 1_000L), loaded());
        // Every family with a source is actually in play, so this is not an empty set agreeing with an
        // empty set: the count is asserted before the two are compared. Failures are the one family with
        // no source to wire -- they are counted from a cause handed over on a witnessing pass, so they are
        // absent here and are covered by the case that feeds one.
        assertThat(measured).hasSize(14);
        FlatMetricProjection flat = FlatMetricProjection.of(measured, ObservationPublisher.FLAT_REDUCTIONS);
        // The load's two measurements are the only ones this face cannot carry, and it says so rather than
        // letting them go missing. They are not lost with them: the same observation carries the same load
        // as its own snapshot dataset, asserted below, which is the face this drop points at. Putting them
        // on this one as well would spell one number two ways in one document.
        assertThat(flat.dropped()).containsExactlyInAnyOrder(
                "tapstate.pipeline.snapshot.rows", "tapstate.pipeline.snapshot.rows.total");
        // The families that carry a chain or a namespace as an attribute reach this face squeezed back to
        // one key per value, and the projection says so: a reader of this face is told what they are
        // reading, and a family added with an attribute and no flat spelling lands in the list above.
        assertThat(flat.reduced()).containsExactlyInAnyOrder(
                "tapstate.pipeline.frontier.gap", "tapstate.pipeline.frontier.stall",
                "tapstate.pipeline.nest.entries", "tapstate.pipeline.nest.accesses",
                "tapstate.pipeline.nest.backfills", "tapstate.pipeline.nest.backfill.time",
                "tapstate.pipeline.nest.pending.high_water", "tapstate.pipeline.nest.stored",
                "tapstate.pipeline.nest.dead_lettered",
                "tapstate.pipeline.join.recompute.rows", "tapstate.pipeline.join.recompute.rows.total",
                "tapstate.pipeline.records.driven");

        Observation published = observations.read("orders").orElseThrow();
        // The flat keys, letter for letter. Every reader of this face -- the command line, the end-to-end
        // helpers, the tutorials -- was built against these spellings, and a family whose dimension moved
        // into an attribute keeps the key it had: the change is in what the facts carry, not in what this
        // face says.
        assertThat(published.metrics()).containsOnlyKeys(
                "recordCount",
                "frontierGap.chain-a",
                "frontierStalledMillis.chain-b",
                "nestStateEntries.nest.orders.doc.$root",
                "nestStateAccesses.nest.orders.doc.$root",
                "nestStateBackfills.nest.orders.doc.$root",
                "nestStateBackfillMillis.nest.orders.doc.$root",
                "nestStatePendingHighWater.nest.orders.doc.$root",
                "nestStateStored.nest.orders.doc.$root",
                "nestDeadLettered.nest.orders.doc.$root",
                "joinRecomputeRowsDone.orders.region",
                "joinRecomputeRowsExpected.orders.region");
        assertThat(published.metrics())
                .containsEntry("frontierGap.chain-a", 4L)
                .containsEntry("frontierStalledMillis.chain-b", 61_000L)
                .containsEntry("nestStateStored.nest.orders.doc.$root", 400_000L)
                .containsEntry("nestDeadLettered.nest.orders.doc.$root", 3L);
        assertThat(published.snapshot())
                .containsOnly(entry("orders", new TableSnapshot(90_000L, 120_000L, 75)));
        // The facts on the document are the measured facts, all of them and as measured: what the flat
        // view dropped is here whole, with its table attribute, and what the flat view squeezed is here
        // with every point it squeezed. A projection may carry less; the document does not.
        assertThat(published.facts()).containsExactlyInAnyOrderElementsOf(measured);
        assertThat(published.facts()).extracting(MetricFact::name)
                .contains("tapstate.pipeline.snapshot.rows", "tapstate.pipeline.snapshot.rows.total");
    }

    @Test
    @DisplayName("the facts carry units the flat view has nowhere to put")
    void theUnitTravelsWithTheFactRatherThanTheName() {
        List<MetricFact> measured = everythingWired().facts("orders", PipelineState.RUNNING, AT,
                Map.of("nest.orders.doc.$root", new NestStateReading(4_000L, 900L, 30L, 210L)),
                Map.of("chain-a", 4L), Map.of("chain-b", 61_000L), Map.of(), Map.of(), Map.of(), loaded());

        assertThat(measured).filteredOn(fact -> fact.name().equals("tapstate.pipeline.frontier.stall"))
                .singleElement()
                .satisfies(fact -> {
                    assertThat(fact.unit()).isEqualTo("ms");
                    // The chain is an attribute of the point, and the name carries no chain at all.
                    assertThat(fact.points()).singleElement().satisfies(point -> assertThat(point.attributes())
                            .containsEntry("tapstate.chain.id", "chain-b")
                            .containsEntry("tapstate.pipeline.id", "orders"));
                });
        assertThat(measured).filteredOn(fact -> fact.name().equals("tapstate.pipeline.records.driven"))
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
                Map.of("chain-a", 4L), Map.of("chain-b", 61_000L), Map.of(), Map.of(), Map.of(), loaded());
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
