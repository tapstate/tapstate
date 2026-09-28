package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.QueueReading;
import io.tapstate.core.lifecycle.StageQueueReading;
import io.tapstate.core.lifecycle.Stage;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StageQueueFactsTest {
    @Test
    void queueStagePointsUseTheFixedStageOrderDespiteReversedInputInsertion() {
        Map<String, StageQueueReading.Sample> reversed = new java.util.LinkedHashMap<>();
        Stage[] stages = Stage.values();
        for (int index = stages.length - 1; index >= 0; index--) {
            reversed.put(stages[index].attributeValue(), new StageQueueReading.Sample(
                    new QueueReading(1, 16, 3), Instant.parse("2026-09-29T08:00:00Z")));
        }
        var facts = ObservationPublisher.stageQueueFacts("orders", new StageQueueReading(reversed));
        assertThat(facts).allSatisfy(fact -> assertThat(fact.points())
                .extracting(point -> point.attributes().get(MetricAttributes.STAGE))
                .containsExactly(java.util.Arrays.stream(stages).map(Stage::attributeValue).toArray(String[]::new)));
    }
    @Test
    void nativeSamplesKeepTheirCollectionTimeAndOnlyClosedStageLabels() {
        Instant collected = Instant.parse("2026-09-29T08:00:00Z");
        var facts = ObservationPublisher.stageQueueFacts("orders", new StageQueueReading(Map.of(
                "transform", new StageQueueReading.Sample(new QueueReading(3, 16, 10), collected))));
        assertThat(facts).extracting(fact -> fact.name()).containsExactly(
                "tapstate.pipeline.stage.queue.depth", "tapstate.pipeline.stage.queue.capacity",
                "tapstate.pipeline.stage.queue.high_water");
        assertThat(facts).allSatisfy(fact -> {
            assertThat(fact.type()).isEqualTo(MetricType.GAUGE);
            assertThat(fact.unit()).isEqualTo("{item}");
            assertThat(fact.points()).singleElement().satisfies(point -> {
                assertThat(point.observedAt()).isEqualTo(collected);
                assertThat(point.startTime()).isNull();
                assertThat(point.attributes()).isEqualTo(Map.of(MetricAttributes.PIPELINE_ID, "orders",
                        MetricAttributes.STAGE, "transform"));
            });
        });
        assertThat(facts.getFirst().points().getFirst().value()).isEqualTo(3);
        assertThat(facts.get(1).points().getFirst().value()).isEqualTo(16);
        assertThat(facts.getLast().points().getFirst().value()).isEqualTo(10);
        assertThat(ObservationPublisher.stageQueueFacts("orders", StageQueueReading.NONE)).isEmpty();
        assertThat(ObservationPublisher.stageQueueFacts("orders", null)).isEmpty();
    }
}
