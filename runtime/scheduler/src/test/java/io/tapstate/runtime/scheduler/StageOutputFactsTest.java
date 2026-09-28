package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.StageOutputReading;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StageOutputFactsTest {
    private static final Instant START = Instant.parse("2026-09-29T07:59:00Z");
    private static final Instant AT = Instant.parse("2026-09-29T08:00:00Z");

    @Test
    void countersAndCompletedRetryDistributionsKeepProducerStartAndSampleTime() {
        var facts = ObservationPublisher.stageOutputFacts("orders", new StageOutputReading(Map.of(
                "transform", new StageOutputReading.Sample(3, retry(), START, AT))));
        assertThat(facts).extracting(fact -> fact.name()).containsExactly(
                "tapstate.pipeline.stage.output.refused", "tapstate.pipeline.stage.output.retry.duration");
        assertThat(facts.getFirst().type()).isEqualTo(MetricType.COUNTER);
        assertThat(facts.getFirst().unit()).isEqualTo("{offer}");
        assertThat(facts.getFirst().points().getFirst().value()).isEqualTo(3);
        assertThat(facts.getLast().type()).isEqualTo(MetricType.HISTOGRAM);
        assertThat(facts.getLast().unit()).isEqualTo("s");
        assertThat(facts.getLast().points().getFirst().histogram().count()).isEqualTo(1);
        assertThat(facts).allSatisfy(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
            assertThat(point.startTime()).isEqualTo(START);
            assertThat(point.observedAt()).isEqualTo(AT);
            assertThat(point.attributes()).isEqualTo(Map.of(MetricAttributes.PIPELINE_ID, "orders",
                    MetricAttributes.STAGE, "transform"));
        }));
    }

    @Test
    void aRefusalStillWaitingForSuccessHasNoCompletedRetryHistogram() {
        var facts = ObservationPublisher.stageOutputFacts("orders", new StageOutputReading(Map.of(
                "source", new StageOutputReading.Sample(2, null, START, AT))));
        assertThat(facts).singleElement().satisfies(fact -> {
            assertThat(fact.name()).isEqualTo("tapstate.pipeline.stage.output.refused");
            assertThat(fact.points().getFirst().value()).isEqualTo(2);
        });
        assertThat(ObservationPublisher.stageOutputFacts("orders", StageOutputReading.NONE)).isEmpty();
        assertThat(ObservationPublisher.stageOutputFacts("orders", null)).isEmpty();
    }

    @Test
    void outputPointsUseTheFixedStageOrderDespiteReversedInputInsertion() {
        Map<String, StageOutputReading.Sample> reversed = new LinkedHashMap<>();
        Stage[] stages = Stage.values();
        for (int index = stages.length - 1; index >= 0; index--) {
            if (stages[index] != Stage.SINK) {
                reversed.put(stages[index].attributeValue(), new StageOutputReading.Sample(3, retry(), START, AT));
            }
        }
        var facts = ObservationPublisher.stageOutputFacts("orders", new StageOutputReading(reversed));
        assertThat(facts).allSatisfy(fact -> assertThat(fact.points())
                .extracting(point -> point.attributes().get(MetricAttributes.STAGE))
                .containsExactly(java.util.Arrays.stream(stages).filter(stage -> stage != Stage.SINK)
                        .map(Stage::attributeValue).toArray(String[]::new)));
    }

    private static HistogramValue retry() {
        HistogramBounds bounds = HistogramBounds.STAGE_OUTPUT_RETRY_DURATION;
        var counts = new ArrayList<>(Collections.nCopies(bounds.buckets(), 0L));
        counts.set(0, 1L);
        return bounds.value(1, 0.0001, counts);
    }
}
