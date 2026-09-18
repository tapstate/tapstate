package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The closed set the {@code stage} attribute is held to, and the two closed sets beside it. The values
 * here are a copy of the design document's lists on purpose: a value added in the code without the
 * document, or the other way round, reddens this rather than shipping two vocabularies.
 *
 * <p>A closed set is only closed if something refuses a value outside it, so the rest of these are the
 * refusals: a point whose direction, operation or stage names a value the set does not hold is refused
 * where its fact is built, while an attribute with no closed set takes whatever the data names.
 */
class WhereInTheGraphTimeIsSpentTest {

    private static final Instant STARTED = Instant.parse("2026-09-17T00:00:00Z");
    private static final Instant OBSERVED = Instant.parse("2026-09-17T00:05:00Z");

    /** One stage's distribution over the registered bounds, with one observation in the first bucket. */
    private static MetricFact spentIn(String stage) {
        List<Long> counts = new ArrayList<>(Collections.nCopies(HistogramBounds.PROCESS_DURATION.buckets(), 0L));
        counts.set(0, 1L);
        return MetricFact.single("tapstate.pipeline.process.duration", MetricType.HISTOGRAM, "s",
                MetricPoint.distribution(Map.of("tapstate.pipeline.id", "orders_sync", "stage", stage),
                        STARTED, OBSERVED, HistogramBounds.PROCESS_DURATION.value(1, 0.002, counts)));
    }

    /** Rows crossing one boundary of one table, with the given direction and operation. */
    private static MetricFact rows(String direction, String op) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                MetricPoint.accumulated(Map.of("tapstate.table.id", "orders", "direction", direction,
                        "op", op), STARTED, OBSERVED, 7L));
    }

    @Test
    @DisplayName("the stages are the five processor families the engine draws a vertex with")
    void theStagesAreTheProcessorFamilies() {
        assertThat(Stage.attributeValues())
                .containsExactlyInAnyOrder("source", "transform", "nest", "join", "sink");
        assertThat(Stage.SINK.attributeValue()).isEqualTo("sink");
    }

    @Test
    @DisplayName("every stage in the set builds a duration under its name")
    void everyStageBuilds() {
        for (Stage stage : Stage.values()) {
            assertThat(spentIn(stage.attributeValue()).points()).singleElement()
                    .satisfies(point -> assertThat(point.attributes()).containsEntry("stage",
                            stage.attributeValue()));
        }
    }

    @Test
    @DisplayName("a duration under a stage outside the set is refused where its fact is built")
    void aStageOutsideTheClosedSetIsRefused() {
        assertThatThrownBy(() -> spentIn("write"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stage=write")
                .hasMessageContaining("closed set")
                .hasMessageContaining("sink");
    }

    @Test
    @DisplayName("direction and operation are closed the same way, and a wire symbol is not a name")
    void directionAndOperationAreClosedTheSameWay() {
        assertThat(rows("in", "read").points()).hasSize(1);

        assertThatThrownBy(() -> rows("outbound", "insert"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("direction=outbound")
                .hasMessageContaining("[in, out]");
        assertThatThrownBy(() -> rows("in", "r"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("op=r")
                .hasMessageContaining("read");
    }

    @Test
    @DisplayName("an attribute with no closed set takes whatever value the data names")
    void anOpenAttributeIsFree() {
        MetricFact fact = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(Map.of("tapstate.table.id", "any.table$name"), OBSERVED, 12L));

        assertThat(fact.points()).hasSize(1);
        assertThat(MetricAttributes.closedDomain("tapstate.table.id")).isEmpty();
    }

    @Test
    @DisplayName("the closed sets are exactly these three")
    void theClosedSetsAreExactlyThese() {
        assertThat(MetricAttributes.closedDomain("direction")).contains(MetricAttributes.DIRECTIONS);
        assertThat(MetricAttributes.DIRECTIONS).containsExactlyInAnyOrder("in", "out");
        assertThat(MetricAttributes.closedDomain("op")).contains(MetricAttributes.OPS);
        assertThat(MetricAttributes.OPS)
                .containsExactlyInAnyOrder("insert", "update", "delete", "read", "ddl", "other");
        assertThat(MetricAttributes.closedDomain("stage")).contains(Stage.attributeValues());
        assertThat(MetricAttributes.closedDomain("code")).isEmpty();
        assertThat(MetricAttributes.closedDomain("tapstate.pipeline.id")).isEmpty();
    }
}
