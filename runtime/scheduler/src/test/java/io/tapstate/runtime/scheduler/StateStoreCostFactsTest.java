package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.StateStoreCostReading;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** One complete job cost reading becomes bounded, typed facts without member or state-key labels. */
class StateStoreCostFactsTest {

    @Test
    void completedAndFailedCallsKeepTheirOutcomeAndOneJobStart() {
        Instant start = Instant.parse("2026-09-27T08:00:00Z");
        Instant observed = start.plusSeconds(30);
        String namespace = "join.orders.widen.fact";
        StateStoreCostReading reading = new StateStoreCostReading(start,
                Map.of("load", new StateStoreCostReading.OperationCost(1, 0, 1_000, 40),
                        "save", new StateStoreCostReading.OperationCost(2, 1, 7_000, 120)),
                Map.of("encode", new StateStoreCostReading.CodecCost(2, 120),
                        "decode", new StateStoreCostReading.CodecCost(1, 40)));

        List<MetricFact> facts = ObservationPublisher.stateCostFacts(
                "orders", observed, Map.of(namespace, reading));

        assertThat(facts).extracting(MetricFact::name).containsExactlyInAnyOrder(
                "tapstate.pipeline.state.store.operation.count",
                "tapstate.pipeline.state.store.operation.duration.sum",
                "tapstate.pipeline.state.store.operation.payload.bytes",
                "tapstate.pipeline.state.store.serialization.count",
                "tapstate.pipeline.state.store.serialization.bytes");
        assertThat(facts).allSatisfy(fact -> {
            assertThat(fact.type()).isEqualTo(MetricType.COUNTER);
            assertThat(fact.points()).allSatisfy(point -> {
                assertThat(point.startTime()).isEqualTo(start);
                assertThat(point.observedAt()).isEqualTo(observed);
                assertThat(point.attributes()).containsEntry(MetricAttributes.PIPELINE_ID, "orders")
                        .containsEntry(MetricAttributes.STATE_NAMESPACE, namespace)
                        .doesNotContainKeys("member", "job", "state.key");
            });
        });
        MetricFact calls = fact(facts, "tapstate.pipeline.state.store.operation.count");
        assertThat(calls.unit()).isEqualTo("{operation}");
        assertThat(calls.points()).hasSize(3)
                .anySatisfy(point -> {
                    assertThat(point.attributes()).containsEntry(MetricAttributes.STATE_OPERATION, "save")
                            .containsEntry(MetricAttributes.STATE_OUTCOME, "failure");
                    assertThat(point.value()).isEqualTo(1);
                });
        assertThat(fact(facts, "tapstate.pipeline.state.store.operation.duration.sum").unit())
                .isEqualTo("ns");
        assertThat(fact(facts, "tapstate.pipeline.state.store.operation.payload.bytes").points())
                .anySatisfy(point -> {
                    assertThat(point.attributes()).containsEntry(MetricAttributes.STATE_OPERATION, "save");
                    assertThat(point.value()).isEqualTo(120);
                });
        assertThat(fact(facts, "tapstate.pipeline.state.store.serialization.bytes").points())
                .anySatisfy(point -> {
                    assertThat(point.attributes()).containsEntry(MetricAttributes.STATE_CODEC, "decode");
                    assertThat(point.value()).isEqualTo(40);
                });
        assertThat(ObservationPublisher.stateCostFacts("orders", observed, Map.of())).isEmpty();
    }

    private static MetricFact fact(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name)).findFirst().orElseThrow();
    }
}
