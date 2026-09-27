package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.runtime.engine.StoredCountSampler;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StoredCountFactsTest {

    private static final Instant START = Instant.parse("2026-09-27T00:00:00Z");
    private static final Instant AT = START.plusSeconds(30);

    @Test
    void quietSamplerHasNoClaimedMeasurements() {
        assertThat(StoredCountFacts.snapshot(new StoredCountSampler.Health(0, 0, 0, 0, 0, 0),
                START, AT)).isEmpty();
    }

    @Test
    void completedAndFailedQueriesExposeCostWithoutPipelineOrNamespaceLabels() {
        Map<String, MetricFact> facts = StoredCountFacts.snapshot(
                        new StoredCountSampler.Health(3, 1, 2, 4, 2, 18_000_000), START, AT)
                .stream().collect(Collectors.toMap(MetricFact::name, Function.identity()));

        assertThat(facts).hasSize(6);
        assertReading(facts, "tapstate.process.nest.stored_count.completed", MetricType.COUNTER, 3);
        assertReading(facts, "tapstate.process.nest.stored_count.failed", MetricType.COUNTER, 1);
        assertReading(facts, "tapstate.process.nest.stored_count.rejected", MetricType.COUNTER, 2);
        assertReading(facts, "tapstate.process.nest.stored_count.duration.sum", MetricType.COUNTER, 18_000_000);
        assertReading(facts, "tapstate.process.nest.stored_count.queued", MetricType.GAUGE, 4);
        assertReading(facts, "tapstate.process.nest.stored_count.active", MetricType.GAUGE, 2);
        assertThat(facts.values()).allSatisfy(fact ->
                assertThat(fact.points().getFirst().attributes()).isEmpty());
    }

    private static void assertReading(Map<String, MetricFact> facts, String name, MetricType type, long value) {
        MetricFact fact = facts.get(name);
        assertThat(fact).isNotNull();
        assertThat(fact.type()).isEqualTo(type);
        assertThat(fact.points().getFirst().value()).isEqualTo(value);
        assertThat(fact.points().getFirst().startTime()).isEqualTo(type == MetricType.COUNTER ? START : null);
        assertThat(fact.points().getFirst().observedAt()).isEqualTo(AT);
    }
}
