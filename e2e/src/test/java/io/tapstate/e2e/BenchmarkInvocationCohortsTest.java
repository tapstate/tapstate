package io.tapstate.e2e;

import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static io.tapstate.e2e.BenchmarkJdiCostObserver.Count;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Namespace;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Operation;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Unit;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.WireKey;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.CallbackKey;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.CostKey;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.Origin;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.Segment;
import static io.tapstate.e2e.BenchmarkJdiTelemetrySession.WireCostKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Carry-in is pending work, while each later invocation retains its own entry cohort. */
class BenchmarkInvocationCohortsTest {
    @Test
    void encoderCostsBalanceEveryEntryCohort() {
        verifyCohorts(segment -> new CostKey(segment, Origin.READ, Namespace.OBSERVATION,
                Unit.BSON_BINARY_ENCODER_INVOCATION));
    }

    @Test
    void wireCommandsBalanceEveryEntryCohort() {
        verifyCohorts(segment -> new WireCostKey(segment, Origin.ROLLUP_BATCH,
                new WireKey(Namespace.RAW_HISTORY, Operation.FIND)));
    }

    @Test
    void callbacksBalanceEveryEntryCohort() {
        verifyCohorts(segment -> new CallbackKey(segment, Origin.JANITOR_BATCH, Namespace.OBSERVATION));
    }

    private static <K> void verifyCohorts(Function<Segment, K> keys) {
        var cohorts = new BenchmarkInvocationCohorts<K>();
        var completedWarmup = cohorts.enter(keys);
        cohorts.returned(completedWarmup);
        assertThat(cohorts.counts()).as("completed warmup is outside the measured cohorts").isEmpty();
        var carry = cohorts.enter(keys);
        assertThat(carry.key()).isEqualTo(keys.apply(Segment.CARRY_IN));
        Map<K, Long> pendingAtBegin = cohorts.pending();
        cohorts.begin();
        assertThat(cohorts.carryIn()).containsExactlyEntriesOf(pendingAtBegin)
                .containsEntry(keys.apply(Segment.CARRY_IN), 1L);
        assertThat(cohorts.counts()).as("carry-in is not fabricated as an observed entry").isEmpty();
        var window = cohorts.enter(keys);
        assertThat(window.key()).isEqualTo(keys.apply(Segment.WINDOW));
        cohorts.returned(carry);
        assertThat(cohorts.counts()).containsEntry(keys.apply(Segment.CARRY_IN), new Count(0, 1));
        assertThat(cohorts.pending()).containsOnly(Map.entry(keys.apply(Segment.WINDOW), 1L));
        cohorts.cutoff();
        assertThatThrownBy(cohorts::requireDrained).isInstanceOf(AssertionError.class);
        var tail = cohorts.enter(keys);
        assertThat(tail.key()).isEqualTo(keys.apply(Segment.DRAIN_TAIL));
        cohorts.returned(window);
        assertThat(cohorts.pending()).containsOnly(Map.entry(keys.apply(Segment.DRAIN_TAIL), 1L));
        cohorts.returned(tail);
        cohorts.requireDrained();
        assertThat(cohorts.pending()).isEmpty();
        assertThat(cohorts.counts()).containsOnly(
                Map.entry(keys.apply(Segment.CARRY_IN), new Count(0, 1)),
                Map.entry(keys.apply(Segment.WINDOW), new Count(1, 1)),
                Map.entry(keys.apply(Segment.DRAIN_TAIL), new Count(1, 1)));
        assertThatThrownBy(() -> cohorts.returned(window)).isInstanceOf(AssertionError.class);
    }
}
