package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservationContinuationTest {
    private static final Instant T0 = Instant.parse("2026-10-01T10:00:00.123456789Z");
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope("inc-a", 41);
    private static final ObservationStore.Scope TARGET = new ObservationStore.Scope("inc-a", 42);
    private static final StopReservation.JobIdentity JOB = new StopReservation.JobIdentity("cluster-a", 77, "boot-a");

    @Test
    void unknownAndAdmittedOnlyCarriersCannotClaimNativeProducerState() {
        var unknown = new ObservationContinuation("handoff", null, Optional.empty(), Optional.empty(), List.of(), List.of());
        assertThat(unknown.knownBaseline()).isFalse();
        assertThatThrownBy(() -> new ObservationContinuation("handoff", SOURCE,
                Optional.of(new ObservationContinuation.Target(TARGET, Optional.empty())), Optional.empty(),
                List.of(counter(7)), List.of(state(9)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObservationContinuation("handoff", null, Optional.empty(), Optional.empty(),
                List.of(counter(7)), List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourceBaselineAndProducerOffsetsKeepPublicStartsSeparateFromRealNativeEpoch() {
        var known = new ObservationContinuation("handoff", SOURCE,
                Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))), Optional.empty(),
                List.of(counter(7)), List.of(state(9)));
        assertThat(known.knownBaseline()).isTrue();
        assertThat(known.producerStates().getFirst().nativeStart()).isEqualTo(T0.plusSeconds(1));
        assertThat(known.producerStates().getFirst().published().getFirst().startTime()).isEqualTo(T0);
    }

    @Test
    void gaugesDuplicateProducerGroupsAndOverBudgetPointsAreRejected() {
        MetricFact gauge = MetricFact.single("tapstate.pipeline.work.active", MetricType.GAUGE, "{work}",
                MetricPoint.reading(Map.of(MetricAttributes.PIPELINE_ID, "orders"), T0, 1));
        assertThatThrownBy(() -> new ObservationContinuation("handoff", SOURCE, Optional.empty(), Optional.empty(),
                List.of(gauge), List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObservationContinuation("handoff", SOURCE,
                Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))), Optional.empty(),
                List.of(counter(7)), List.of(state(9), state(9)))).isInstanceOf(IllegalArgumentException.class);
        var points = java.util.stream.IntStream.range(0, 1002).mapToObj(index -> MetricPoint.accumulated(
                Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.TABLE_ID, "table-" + index,
                        MetricAttributes.DIRECTION, "out"), T0, T0, 1)).toList();
        var unbounded = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", points);
        assertThatThrownBy(() -> new ObservationContinuation("handoff", SOURCE, Optional.empty(), Optional.empty(),
                List.of(unbounded), List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aReceiptQualifiesTheActualCommittedTargetWithoutComparingPointTimestamps() {
        var receipt = new ObservationStore.ContinuationReceipt("orders", "revision", "a".repeat(64), 3,
                "handoff", SOURCE, Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))),
                Optional.empty(), true);
        assertThat(receipt.matches(new HandoffIdentity("orders", "handoff", StopReservation.CounterPolicy.CONTINUE,
                SOURCE, TARGET, JOB))).isTrue();
        assertThat(receipt.matches(new HandoffIdentity("orders", "other", StopReservation.CounterPolicy.CONTINUE,
                SOURCE, TARGET, JOB))).isFalse();
        assertThat(receipt.matches(new HandoffIdentity("orders", "handoff", StopReservation.CounterPolicy.RESET,
                SOURCE, TARGET, JOB))).isFalse();
    }

    private static MetricFact counter(long value) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}", point(value));
    }
    private static MetricPoint point(long value) {
        return MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.TABLE_ID, "table-a",
                MetricAttributes.DIRECTION, "out"), T0, T0, value);
    }
    private static ObservationContinuation.ProducerState state(long published) {
        return new ObservationContinuation.ProducerState("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                "out", "", T0.plusSeconds(1), List.of(), List.of(point(published)));
    }
}
