package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.ObservationStore;
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
        private final List<ObservationStore.Scope> scopes = new ArrayList<>();
        private final List<Instant> gapStarts = new ArrayList<>();
        private int failuresRemaining;
        private java.util.function.Consumer<RateSample> beforeAppend = sample -> { };

        @Override
        public void append(RateSample sample) {
            append(sample, null);
        }

        @Override
        public void append(RateSample sample, Instant gapFrom) {
            if (failuresRemaining > 0) {
                failuresRemaining--;
                throw new IllegalStateException("injected append failure");
            }
            beforeAppend.accept(sample);
            appended.add(sample);
            gapStarts.add(gapFrom);
        }

        @Override
        public void appendScoped(RateSample sample, ObservationStore.Scope scope) {
            appendScoped(sample, scope, null);
        }

        @Override
        public void appendScoped(RateSample sample, ObservationStore.Scope scope, Instant gapFrom) {
            append(sample, gapFrom);
            scopes.add(scope);
        }

        @Override
        public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
            return new Page(List.of(), false);
        }

        @Override
        public java.util.Optional<Entry> predecessor(String pipelineId, Instant at) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<Entry> read(String pipelineId, Key key) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<Entry> successor(String pipelineId, Instant at) {
            return java.util.Optional.empty();
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

    @Test
    void aNewExecutionStartsItsOwnCadenceAndWritesTheOwnerWithTheSample() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope first = new ObservationStore.Scope("inc-old", 41);
        ObservationStore.Scope recreated = new ObservationStore.Scope("inc-new", 42);
        ObservationStore.Scope restarted = new ObservationStore.Scope("inc-new", 43);

        assertThat(sampler.appendIfDue(moving(T0, 1), first)).isTrue();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(1), 2), first)).isFalse();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(2), 3), recreated)).isTrue();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(3), 4), restarted)).isTrue();

        assertThat(history.scopes).containsExactly(first, recreated, restarted);
        assertThat(history.appended).extracting(sample -> sample.counters().get("records.out"))
                .containsExactly(1L, 3L, 4L);
    }

    @Test
    void repeatedFailuresKeepOnlyTheFirstLostTimeUntilRecovery() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        history.failuresRemaining = 2;

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sampler.appendIfDue(moving(T0, 1)))
                .hasMessage("injected append failure");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sampler.appendIfDue(moving(T0.plusSeconds(1), 2)))
                .hasMessage("injected append failure");
        assertThat(sampler.gapHealth().open()).isEqualTo(1);
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).isZero();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(2), 3))).isTrue();
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).isEqualTo(1);
        assertThat(history.gapStarts).containsExactly(T0);
        assertThat(history.appended).hasSize(1);
    }

    @Test
    void aRetryAtTheSameSampleTimeDoesNotLeaveAFalseFutureGap() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        history.failuresRemaining = 1;

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sampler.appendIfDue(moving(T0, 1)))
                .hasMessage("injected append failure");
        assertThat(sampler.appendIfDue(moving(T0, 1))).isTrue();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(60), 2))).isTrue();

        assertThat(history.gapStarts).containsExactly(null, null);
    }


    @Test
    void scopedRecoveryWritesTheGapInTheSameExecution() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
        sampler.appendIfDue(moving(T0, 1), run);
        history.failuresRemaining = 1;
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                sampler.appendIfDue(moving(T0.plusSeconds(60), 2), run))
                .hasMessage("injected append failure");

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(61), 3), run)).isTrue();
        assertThat(history.scopes).containsExactly(run, run);
        assertThat(history.gapStarts).containsExactly(null, T0.plusSeconds(60));
    }

    @Test
    void anOldExecutionFailureDoesNotMarkTheNextExecution() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope old = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Scope next = new ObservationStore.Scope("inc-a", 42);
        sampler.appendIfDue(moving(T0, 1), old);
        history.failuresRemaining = 1;
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                sampler.appendIfDue(moving(T0.plusSeconds(60), 2), old))
                .hasMessage("injected append failure");

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(61), 3), next)).isTrue();
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).as("new execution is not recovery of the old gap").isZero();
        assertThat(history.scopes).containsExactly(old, next);
        assertThat(history.gapStarts).containsExactly(null, null);
    }

    @Test
    void droppingMoreFramesExtendsOneGapAndForgettingItDoesNotClaimRecovery() {
        RateSampler sampler = new RateSampler(new RecordingHistory(), Duration.ofSeconds(60));
        ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
        sampler.markDropped(moving(T0, 1), run);
        sampler.markDropped(moving(T0.plusSeconds(1), 2), run);
        assertThat(sampler.gapHealth().open()).isEqualTo(1);
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).isZero();

        sampler.forgetPipelinesOutside(List.of());
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().closed()).isZero();
        sampler.markDropped(moving(T0.plusSeconds(2), 3), run);
        assertThat(sampler.gapHealth().open()).isEqualTo(1);
        assertThat(sampler.gapHealth().opened()).isEqualTo(2);
        assertThat(sampler.gapHealth().closed()).isZero();
    }

    @Test
    void aNewExecutionsFailureStartsItsOwnGapWithoutRecoveringTheOldOne() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope old = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Scope next = new ObservationStore.Scope("inc-a", 42);
        history.failuresRemaining = 2;
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sampler.appendIfDue(moving(T0, 1), old))
                .hasMessage("injected append failure");
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                sampler.appendIfDue(moving(T0.plusSeconds(1), 2), next))
                .hasMessage("injected append failure");
        assertThat(sampler.gapHealth().open()).isEqualTo(1);
        assertThat(sampler.gapHealth().opened()).isEqualTo(2);
        assertThat(sampler.gapHealth().closed()).isZero();

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(2), 3), next)).isTrue();
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().opened()).isEqualTo(2);
        assertThat(sampler.gapHealth().closed()).isEqualTo(1);
        assertThat(history.gapStarts).containsExactly(T0.plusSeconds(1));
    }

    @Test
    void preparationLossWithoutAnObservationKeepsItsRealTimeUntilALineBearingSampleSucceeds() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
        Instant lostAt = T0.plusMillis(37);

        sampler.markPreparationDropped("orders", run, lostAt);
        sampler.markPreparationDropped("orders", run, T0.plusMillis(38));
        assertThat(history.appended).isEmpty();
        assertThat(history.gapStarts).isEmpty();
        assertThat(sampler.hasOpenGap("orders", run)).isTrue();
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).isZero();

        Observation empty = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, T0.plusMillis(39));
        assertThat(sampler.appendIfDue(empty, run)).isFalse();
        assertThat(sampler.hasOpenGap("orders", run)).isTrue();
        history.failuresRemaining = 1;
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                sampler.appendIfDue(moving(T0.plusSeconds(1), 2), run)).hasMessage("injected append failure");
        assertThat(history.appended).isEmpty();
        assertThat(sampler.hasOpenGap("orders", run)).isTrue();
        assertThat(sampler.gapHealth().closed()).isZero();
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(2), 3), run)).isTrue();
        assertThat(history.gapStarts).containsExactly(lostAt);
        assertThat(history.scopes).containsExactly(run);
        assertThat(history.appended).singleElement().satisfies(sample -> {
            assertThat(sample.pipelineId()).isEqualTo("orders");
            assertThat(sample.observedAt()).isEqualTo(T0.plusSeconds(2));
            assertThat(sample.counters()).containsEntry("records.out", 3L);
            assertThat(sample.countingSince()).isEqualTo(STARTED);
        });
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().closed()).isEqualTo(1);
    }

    @Test
    void preparationLossUsesTheExistingIntervalAndDoesNotAdvanceTheSamplingCadence() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
        sampler.appendIfDue(moving(T0, 1), run);

        sampler.markPreparationDropped("orders", run, T0.plusMillis(59_999));
        assertThat(sampler.hasOpenGap("orders", run)).isFalse();
        sampler.markPreparationDropped("orders", run, T0.plusSeconds(60));
        assertThat(sampler.appendIfDue(moving(T0.plusMillis(59_999), 2), run)).isFalse();
        assertThat(sampler.hasOpenGap("orders", run)).isTrue();
        assertThat(history.appended).hasSize(1);
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(61), 3), run)).isTrue();
        assertThat(history.gapStarts).containsExactly(null, T0.plusSeconds(60));
        assertThat(history.appended).extracting(RateSample::observedAt).containsExactly(T0, T0.plusSeconds(61));
    }

    @Test
    void latePreparationLossFromAnOldEqualGenerationOrLegacyOwnerCannotPoisonTheCurrentGap() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope current = new ObservationStore.Scope("inc-new", 42);
        sampler.appendIfDue(moving(T0, 1), current);

        sampler.markPreparationDropped("orders", new ObservationStore.Scope("inc-old", 41), T0.plusSeconds(120));
        sampler.markPreparationDropped("orders", new ObservationStore.Scope("inc-other", 42), T0.plusSeconds(121));
        sampler.markPreparationDropped("orders", null, T0.plusSeconds(122));
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().opened()).isZero();

        sampler.markPreparationDropped("orders", current, T0.plusSeconds(60));
        sampler.markPreparationDropped("orders", new ObservationStore.Scope("inc-old", 41), T0.plusSeconds(10));
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(61), 3), current)).isTrue();
        assertThat(history.scopes).containsExactly(current, current);
        assertThat(history.gapStarts).containsExactly(null, T0.plusSeconds(60));
    }

    @Test
    void replacingAPreparationGapDoesNotRecoverItOrTransferItToANewExecution() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope old = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Scope next = new ObservationStore.Scope("inc-b", 42);
        sampler.markPreparationDropped("orders", old, T0);

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(1), 2), next)).isTrue();
        sampler.markPreparationDropped("orders", old, T0.plusSeconds(120));
        assertThat(history.gapStarts).containsExactly((Instant) null);
        assertThat(history.scopes).containsExactly(next);
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).isZero();
    }

    @Test
    void aNewExecutionPreparationLossStartsItsOwnCadenceAndGap() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope old = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Scope next = new ObservationStore.Scope("inc-a", 42);
        sampler.appendIfDue(moving(T0, 1), old);
        sampler.markPreparationDropped("orders", next, T0.plusSeconds(1));

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(2), 3), next)).isTrue();
        assertThat(history.scopes).containsExactly(old, next);
        assertThat(history.gapStarts).containsExactly(null, T0.plusSeconds(1));
        assertThat(sampler.gapHealth().opened()).isEqualTo(1);
        assertThat(sampler.gapHealth().closed()).isEqualTo(1);
    }

    @Test
    void legacyPreparationLossKeepsTheOriginalCadenceWithoutWritingAFrame() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        sampler.appendIfDue(moving(T0, 1));
        sampler.markPreparationDropped("orders", null, T0.plusSeconds(59));
        assertThat(sampler.gapHealth().open()).isZero();
        sampler.markPreparationDropped("orders", null, T0.plusSeconds(60));
        assertThat(history.appended).hasSize(1);

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(61), 3))).isTrue();
        assertThat(history.scopes).isEmpty();
        assertThat(history.gapStarts).containsExactly(null, T0.plusSeconds(60));
        assertThat(sampler.gapHealth().closed()).isEqualTo(1);
    }

    @Test
    void aLegacyPreparationGapCannotAttachToAScopedSampleOrBeReopenedByALateLegacySignal() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        sampler.markPreparationDropped("orders", null, T0);
        ObservationStore.Scope current = new ObservationStore.Scope("inc-a", 41);

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(1), 2), current)).isTrue();
        sampler.markPreparationDropped("orders", null, T0.plusSeconds(120));
        assertThat(history.gapStarts).containsExactly((Instant) null);
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().closed()).isZero();
    }

    @Test
    void anOlderSuccessfulAppendCannotEraseAPreparationLossObservedDuringItsStoreCall() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope run = new ObservationStore.Scope("inc-a", 41);
        sampler.appendIfDue(moving(T0, 1), run);
        history.beforeAppend = sample -> sampler.markPreparationDropped("orders", run, T0.plusSeconds(65));

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(60), 2), run)).isTrue();
        assertThat(sampler.hasOpenGap("orders", run)).isTrue();
        history.beforeAppend = sample -> { };
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(120), 3), run)).isTrue();
        assertThat(history.gapStarts).containsExactly(null, null, T0.plusSeconds(65));
        assertThat(sampler.gapHealth().closed()).isEqualTo(1);
    }

    @Test
    void aLateOldPreparationSignalDuringANewScopeAppendCannotUndoTheNewCadence() {
        RecordingHistory history = new RecordingHistory();
        RateSampler sampler = new RateSampler(history, Duration.ofSeconds(60));
        ObservationStore.Scope old = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Scope next = new ObservationStore.Scope("inc-a", 42);
        sampler.appendIfDue(moving(T0, 1), old);
        history.beforeAppend = sample -> sampler.markPreparationDropped("orders", old, T0.plusSeconds(60));

        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(1), 2), next)).isTrue();
        history.beforeAppend = sample -> { };
        assertThat(sampler.appendIfDue(moving(T0.plusSeconds(2), 3), next)).isFalse();
        assertThat(history.scopes).containsExactly(old, next);
        assertThat(history.gapStarts).containsExactly(null, null);
        assertThat(sampler.gapHealth().open()).isZero();
        assertThat(sampler.gapHealth().opened()).isZero();
    }
}
