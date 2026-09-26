package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ObservationScopeRegistryTest {

    @Test
    void aRebuildKeepsNamedSeriesWithinTheDeclaredTableBudgetAndPreservesTheirTotal() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-a", 1);
        List<MetricPoint> many = new ArrayList<>();
        for (int table = 0; table < 1_000; table++) {
            many.add(MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                    MetricAttributes.TABLE_ID, "table-%04d".formatted(table)), START,
                    START.plusSeconds(1), 1));
        }
        MetricFact oldFact = new MetricFact("tapstate.pipeline.snapshot.rows", MetricType.COUNTER,
                "{row}", many);
        Observation oldObservation = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, START.plusSeconds(1), List.of(oldFact));
        scopes.continueFrame(new ObservationPublisher.Prepared(oldObservation, false,
                Map.of(), Map.of(), Map.of()), old);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());

        ObservationStore.Scope resumed = scopes.begin(PIPELINE, "inc-a", 2);
        MetricPoint newcomer = MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                MetricAttributes.TABLE_ID, "table-new"), START.plusSeconds(2), START.plusSeconds(2), 2);
        Observation fresh = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                START.plusSeconds(2), List.of(new MetricFact("tapstate.pipeline.snapshot.rows",
                        MetricType.COUNTER, "{row}", List.of(newcomer))));
        ObservationPublisher.Prepared continued = scopes.continueFrame(
                new ObservationPublisher.Prepared(fresh, false, Map.of(), Map.of(), Map.of()), resumed);
        List<MetricPoint> points = continued.observation().facts().getFirst().points();

        assertThat(points).hasSize(1_001);
        assertThat(points).anySatisfy(point -> {
            assertThat(point.attributes()).containsEntry(MetricAttributes.TABLE_ID, "table-0999");
            assertThat(point.value()).isEqualTo(1);
        });
        assertThat(points).anySatisfy(point -> {
            assertThat(point.attributes()).containsEntry(MetricAttributes.OVERFLOW, "true");
            assertThat(point.value()).isEqualTo(2);
        });
        assertThat(points.stream().mapToLong(MetricPoint::value).sum()).isEqualTo(1_002);
    }

    private static final String PIPELINE = "orders";
    private static final Instant START = Instant.parse("2026-09-27T00:00:00Z");
    private static final Map<String, String> TABLE = Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
            MetricAttributes.TABLE_ID, "orders");

    @Test
    void repeatedRebuildsCarryTheCorrectedBaselineWhileAPlainStopStartResetsIt() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(START.plusSeconds(1), START, 7), first);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());

        ObservationStore.Scope second = scopes.begin(PIPELINE, "inc-a", 2);
        ObservationPublisher.Prepared afterOne = scopes.continueFrame(
                frame(START.plusSeconds(2), START.plusSeconds(2), 2), second);
        assertThat(counter(afterOne)).isEqualTo(9);
        assertThat(start(afterOne)).isEqualTo(START);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());

        ObservationStore.Scope third = scopes.begin(PIPELINE, "inc-a", 3);
        ObservationPublisher.Prepared afterTwo = scopes.continueFrame(
                frame(START.plusSeconds(3), START.plusSeconds(3), 3), third);
        assertThat(counter(afterTwo)).isEqualTo(12);
        assertThat(start(afterTwo)).isEqualTo(START);

        scopes.clearContinuation(PIPELINE);
        ObservationPublisher.Prepared stopped = scopes.continueFrame(
                gaugeFrame(START.plusSeconds(3).plusMillis(500), 22, PipelineState.STOPPED), third);
        assertThat(counter(stopped)).isEqualTo(12);
        assertThat(start(stopped)).isEqualTo(START);
        assertThat(stopped.observation().state()).isEqualTo(PipelineState.STOPPED);
        ObservationStore.Scope fresh = scopes.begin(PIPELINE, "inc-a", 4);
        ObservationPublisher.Prepared restarted = scopes.continueFrame(
                frame(START.plusSeconds(4), START.plusSeconds(4), 1), fresh);
        assertThat(counter(restarted)).isEqualTo(1);
        assertThat(start(restarted)).isEqualTo(START.plusSeconds(4));
        assertThat(scopes.continueFrame(frame(START.plusSeconds(5), START, 99), second).observation().facts())
                .isEqualTo(frame(START.plusSeconds(5), START, 99).observation().facts());
        assertThat(scopes.current(PIPELINE)).contains(fresh);
    }

    @Test
    void aRejectedNewExecutionKeepsTheFrozenBaselineForTheNextSubmission() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(START.plusSeconds(1), START, 7), old);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        ObservationStore.Scope rejected = scopes.begin(PIPELINE, "inc-a", 2);
        scopes.discard(PIPELINE, rejected);

        ObservationStore.Scope admitted = scopes.begin(PIPELINE, "inc-a", 3);
        ObservationPublisher.Prepared continued = scopes.continueFrame(
                frame(START.plusSeconds(3), START.plusSeconds(3), 2), admitted);

        assertThat(counter(continued)).isEqualTo(9);
        assertThat(start(continued)).isEqualTo(START);
    }

    @Test
    void anUnknownBaselineStaysUnknownAndAStoredMatchingScopeCanSupplyAKnownOne() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        ObservationStore.Scope noBaseline = scopes.begin(PIPELINE, "inc-a", 2);
        ObservationPublisher.Prepared unknown = scopes.continueFrame(gaugeFrame(START.plusSeconds(2), 22), noBaseline);
        assertThat(unknown.observation().facts()).allMatch(fact -> fact.type() == MetricType.GAUGE);
        assertThat(unknown.observation().metrics()).containsEntry("lag.orders", 22L)
                .doesNotContainKey("records.out");

        scopes.clearContinuation(PIPELINE);
        ObservationStore.Scope storedOwner = scopes.begin(PIPELINE, "inc-a", 3);
        Observation saved = frame(START.plusSeconds(3), START, 7).observation();
        scopes.prepareRebuildingResume(PIPELINE,
                Optional.of(new ObservationStore.Stored(saved, Optional.of(storedOwner))));
        ObservationStore.Scope resumed = scopes.begin(PIPELINE, "inc-a", 4);
        ObservationPublisher.Prepared fromStored = scopes.continueFrame(
                frame(START.plusSeconds(4), START.plusSeconds(4), 2), resumed);
        assertThat(counter(fromStored)).isEqualTo(9);

        scopes.clearContinuation(PIPELINE);
        ObservationStore.Scope other = scopes.begin(PIPELINE, "inc-b", 5);
        scopes.prepareRebuildingResume(PIPELINE,
                Optional.of(new ObservationStore.Stored(saved, Optional.of(old))));
        ObservationStore.Scope newIncarnation = scopes.begin(PIPELINE, "inc-b", 6);
        assertThat(counter(scopes.continueFrame(frame(START.plusSeconds(6), START.plusSeconds(6), 2),
                newIncarnation))).isEqualTo(2);
        assertThat(other.pipelineIncarnationId()).isEqualTo("inc-b");
    }

    private static ObservationPublisher.Prepared frame(Instant at, Instant since, long rows) {
        MetricFact counter = MetricFact.single("tapstate.pipeline.snapshot.rows", MetricType.COUNTER, "{row}",
                MetricPoint.accumulated(TABLE, since, at, rows));
        Observation observation = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, at, List.of(counter));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static ObservationPublisher.Prepared gaugeFrame(Instant at, long lag) {
        return gaugeFrame(at, lag, PipelineState.RUNNING);
    }

    private static ObservationPublisher.Prepared gaugeFrame(Instant at, long lag, PipelineState state) {
        MetricFact gauge = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(TABLE, at, lag));
        Observation observation = new Observation(PIPELINE, state,
                Map.of("lag.orders", lag), Map.of(), Map.of(), null, at, List.of(gauge));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static long counter(ObservationPublisher.Prepared frame) {
        return frame.observation().facts().stream()
                .filter(fact -> fact.name().equals("tapstate.pipeline.snapshot.rows"))
                .findFirst().orElseThrow().points().getFirst().value();
    }

    private static Instant start(ObservationPublisher.Prepared frame) {
        return frame.observation().facts().stream()
                .filter(fact -> fact.name().equals("tapstate.pipeline.snapshot.rows"))
                .findFirst().orElseThrow().points().getFirst().startTime();
    }
}
