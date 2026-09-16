package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flat {@code name -> value} view of a set of metric facts: what survives it, what cannot, and
 * the part that matters most — that what cannot is named rather than simply missing. A metric nobody
 * wired and a metric this view had no room for are the same absence to whoever is reading the face,
 * and the two want opposite reactions.
 */
class AFlatViewSaysWhatItCouldNotCarryTest {

    private static final Instant STARTED = Instant.parse("2026-09-16T00:00:00Z");
    private static final Instant OBSERVED = Instant.parse("2026-09-16T00:05:00Z");

    @Test
    @DisplayName("a single undimensioned point comes through as one entry")
    void anUndimensionedMetricSurvivesTheProjection() {
        FlatMetricProjection projected = FlatMetricProjection.of(List.of(
                MetricFact.single("errorCount", MetricType.GAUGE, "{error}",
                        MetricPoint.reading(Map.of(), OBSERVED, 0L)),
                MetricFact.single("recordCount", MetricType.GAUGE, "{record}",
                        MetricPoint.reading(Map.of(), OBSERVED, 128500L))));

        assertThat(projected.metrics()).containsExactlyInAnyOrderEntriesOf(
                Map.of("errorCount", 0L, "recordCount", 128500L));
        assertThat(projected.dropped()).isEmpty();
    }

    @Test
    @DisplayName("a metric broken out by attributes is dropped, and named")
    void aDimensionedMetricIsDroppedByName() {
        FlatMetricProjection projected = FlatMetricProjection.of(List.of(
                MetricFact.single("errorCount", MetricType.GAUGE, "{error}",
                        MetricPoint.reading(Map.of(), OBSERVED, 0L)),
                new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                        List.of(MetricPoint.accumulated(Map.of("direction", "in"), STARTED, OBSERVED, 9L),
                                MetricPoint.accumulated(Map.of("direction", "out"), STARTED, OBSERVED,
                                        8L)))));

        // The undimensioned one still arrives: the drop is this metric's, not the whole projection
        // falling over, which would be a different defect wearing the same empty map.
        assertThat(projected.metrics()).containsOnlyKeys("errorCount");
        assertThat(projected.dropped()).containsExactly("tapstate.pipeline.records");
    }

    @Test
    @DisplayName("a single point that carries one attribute is dropped too")
    void oneAttributeIsAlreadyMoreThanANameCanHold() {
        FlatMetricProjection projected = FlatMetricProjection.of(List.of(
                MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                        MetricPoint.reading(Map.of("tapstate.table.id", "orders"), OBSERVED, 12L))));

        assertThat(projected.metrics()).isEmpty();
        assertThat(projected.dropped()).containsExactly("tapstate.pipeline.lag");
    }

    @Test
    @DisplayName("a distribution has no single number to be, so it is dropped and named")
    void aHistogramIsDroppedByName() {
        FlatMetricProjection projected = FlatMetricProjection.of(List.of(
                MetricFact.single("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM,
                        "s", MetricPoint.distribution(Map.of(), STARTED, OBSERVED,
                                new HistogramValue(3, 1.5, List.of(1.0), List.of(2L, 1L))))));

        assertThat(projected.metrics()).isEmpty();
        assertThat(projected.dropped())
                .containsExactly("tapstate.pipeline.record.delivery.duration");
    }

    @Test
    @DisplayName("no facts is an empty view, not a dropped one")
    void nothingMeasuredDropsNothing() {
        FlatMetricProjection projected = FlatMetricProjection.of(List.of());

        assertThat(projected.metrics()).isEmpty();
        assertThat(projected.dropped()).isEmpty();
    }
}
