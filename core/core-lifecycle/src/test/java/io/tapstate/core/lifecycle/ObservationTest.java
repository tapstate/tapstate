package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservationTest {

    @Test
    void holdsThePipelineIdStateMetricsAndSnapshot() {
        Observation obs = new Observation(
                "orders_sync",
                PipelineState.RUNNING,
                Map.of("recordCount", 128500L, "lag", 42000L),
                Map.of("orders", new TableSnapshot(90000L, 120000L, 75)));

        assertThat(obs.pipelineId()).isEqualTo("orders_sync");
        assertThat(obs.state()).isEqualTo(PipelineState.RUNNING);
        assertThat(obs.metrics()).containsEntry("recordCount", 128500L).containsEntry("lag", 42000L);
        assertThat(obs.snapshot()).containsKey("orders");
        assertThat(obs.snapshot().get("orders").rowsDone()).isEqualTo(90000L);
    }

    @Test
    void requiresPipelineIdAndState() {
        assertThatNullPointerException()
                .isThrownBy(() -> new Observation(null, PipelineState.NEW, Map.of(), Map.of()));
        assertThatNullPointerException()
                .isThrownBy(() -> new Observation("p1", null, Map.of(), Map.of()));
    }

    @Test
    void nullMetricsOrSnapshotBecomeEmpty() {
        // A state-only observation is normal (metrics/snapshot sources may not be wired): null reads as
        // empty (unavailable), never a NullPointerException at a read site.
        Observation obs = new Observation("p1", PipelineState.NEW, null, null);

        assertThat(obs.metrics()).isEmpty();
        assertThat(obs.snapshot()).isEmpty();
    }

    @Test
    void metricsAndSnapshotAreDefensivelyCopied() {
        Map<String, Long> metrics = new HashMap<>(Map.of("recordCount", 1L));
        Map<String, TableSnapshot> snapshot = new HashMap<>(Map.of("t", new TableSnapshot(1L, null, null)));
        Observation obs = new Observation("p1", PipelineState.RUNNING, metrics, snapshot);

        metrics.put("recordCount", 999L);
        snapshot.clear();

        assertThat(obs.metrics()).containsEntry("recordCount", 1L);
        assertThat(obs.snapshot()).containsKey("t");
        assertThatThrownBy(() -> obs.metrics().put("x", 1L)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void carriesTargetAckedPositionsAsOpaqueStrings() {
        // The stored position is how far the target confirmed writes: an opaque source position
        // (binlog/GTID/LSN), a String, not a numeric metric — so it rides a separate positions projection,
        // not the Long metrics map. Which of the four positions it is is the read face's word to say.
        Observation obs = new Observation(
                "orders_sync",
                PipelineState.RUNNING,
                Map.of("recordCount", 128500L),
                Map.of(),
                Map.of("orders", "mysql-bin.000007:154732"));

        assertThat(obs.positions()).containsEntry("orders", "mysql-bin.000007:154732");
        assertThat(obs.metrics()).containsEntry("recordCount", 128500L);
    }

    @Test
    void positionsDefaultToEmptyAndAreDefensivelyCopied() {
        // A position source not yet wired reads as empty (unavailable), never a NullPointerException; and the
        // stored projection is immutable, like metrics and snapshot. The four-arg form defaults positions empty.
        Observation viaFourArg = new Observation("p1", PipelineState.NEW, null, null);
        assertThat(viaFourArg.positions()).isEmpty();

        Map<String, String> positions = new HashMap<>(Map.of("t", "pos-1"));
        Observation obs = new Observation("p1", PipelineState.RUNNING, Map.of(), Map.of(), positions);
        positions.put("t", "pos-999");

        assertThat(obs.positions()).containsEntry("t", "pos-1");
        assertThatThrownBy(() -> obs.positions().put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void carriesTheMeasuredFactsBesideTheFlatView() {
        // The flat map is one number per name; the fact under it keeps the dimensions, the kind of
        // measurement and the unit the map has nowhere to put. Both travel, from one measurement.
        Instant at = Instant.parse("2026-09-17T10:00:00Z");
        MetricFact records = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                List.of(MetricPoint.accumulated(
                        Map.of("tapstate.pipeline.id", "orders_sync", "direction", "in"), at, at, 100L)));
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("records.in", 100L), Map.of(), Map.of(), null, at, List.of(records));

        assertThat(obs.facts()).containsExactly(records);
        assertThat(obs.metrics()).containsEntry("records.in", 100L);
    }

    @Test
    void factsDefaultToEmptyAndAreDefensivelyCopied() {
        // Every older constructor and a null list read as "no facts carried" — never as facts derived
        // from the flat map, which would be a measurement nobody took.
        assertThat(new Observation("p1", PipelineState.NEW, null, null).facts()).isEmpty();
        assertThat(new Observation("p1", PipelineState.NEW, Map.of(), Map.of(), Map.of(), null,
                Instant.EPOCH).facts()).isEmpty();
        assertThat(new Observation("p1", PipelineState.NEW, Map.of(), Map.of(), Map.of(), null,
                Instant.EPOCH, null).facts()).isEmpty();

        List<MetricFact> facts = new ArrayList<>(List.of(MetricFact.single("recordCount", MetricType.GAUGE,
                "{record}", MetricPoint.reading(Map.of(), Instant.EPOCH, 1L))));
        Observation obs = new Observation("p1", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                Instant.EPOCH, facts);
        facts.clear();

        assertThat(obs.facts()).hasSize(1);
        assertThatThrownBy(() -> obs.facts().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void refusesTwoFactsUnderOneName() {
        // Two facts for one instrument are two answers to one question; a reader taking either takes one
        // arbitrarily, which is the same failure a fact refuses for two points on the same attributes.
        MetricFact first = MetricFact.single("recordCount", MetricType.GAUGE, "{record}",
                MetricPoint.reading(Map.of(), Instant.EPOCH, 1L));
        MetricFact second = MetricFact.single("recordCount", MetricType.GAUGE, "{record}",
                MetricPoint.reading(Map.of(), Instant.EPOCH, 2L));

        assertThatThrownBy(() -> new Observation("p1", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, Instant.EPOCH, List.of(first, second)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recordCount")
                .hasMessageContaining("twice");
    }

    @Test
    void tableSnapshotAllowsUnavailableTotalAndPct() {
        // rows_total not yet wired: total and pct are null (unavailable), never faked as 0 or 100.
        TableSnapshot unavailable = new TableSnapshot(500L, null, null);

        assertThat(unavailable.rowsDone()).isEqualTo(500L);
        assertThat(unavailable.rowsTotal()).isNull();
        assertThat(unavailable.donePct()).isNull();
    }
}
