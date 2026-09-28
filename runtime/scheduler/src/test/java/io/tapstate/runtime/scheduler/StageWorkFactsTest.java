package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.StageWorkReading;
import io.tapstate.core.lifecycle.Stage;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StageWorkFactsTest {

    @Test
    void activeStagePointsUseTheFixedStageOrderDespiteReversedInputInsertion() {
        Map<String, Long> reversed = new java.util.LinkedHashMap<>();
        Stage[] stages = Stage.values();
        for (int index = stages.length - 1; index >= 0; index--) {
            reversed.put(stages[index].attributeValue(), 1L);
        }
        var facts = ObservationPublisher.activeWorkFacts("orders",
                new StageWorkReading(reversed, Instant.parse("2026-09-29T08:00:00Z")));
        assertThat(facts.getFirst().points()).extracting(point -> point.attributes().get(MetricAttributes.STAGE))
                .containsExactly(java.util.Arrays.stream(stages).map(Stage::attributeValue).toArray(String[]::new));
    }

    @Test
    void completeWorkKeepsItsActualCollectionTimeAndClosedStageLabels() {
        Instant collected = Instant.parse("2026-09-29T08:00:00Z");
        var facts = ObservationPublisher.activeWorkFacts("orders", new StageWorkReading(
                Map.of("transform", 2L, "sink", 1L), collected));
        assertThat(facts).hasSize(2).allSatisfy(fact -> {
            assertThat(fact.type()).isEqualTo(MetricType.GAUGE);
            assertThat(fact.unit()).isEqualTo("{work}");
            assertThat(fact.points()).allSatisfy(point -> {
                assertThat(point.observedAt()).isEqualTo(collected);
                assertThat(point.startTime()).isNull();
                assertThat(point.attributes()).containsEntry(MetricAttributes.PIPELINE_ID, "orders");
            });
        });
        assertThat(facts.getFirst().points()).hasSize(2).allSatisfy(point ->
                assertThat(point.attributes()).hasSize(2).containsKey(MetricAttributes.STAGE));
        assertThat(facts.getLast().points()).singleElement().satisfies(point -> {
            assertThat(point.value()).isEqualTo(3);
            assertThat(point.attributes()).hasSize(1);
        });
        assertThat(ObservationPublisher.activeWorkFacts("orders", StageWorkReading.NONE)).isEmpty();
        assertThat(ObservationPublisher.activeWorkFacts("orders", null)).isEmpty();
    }
}
