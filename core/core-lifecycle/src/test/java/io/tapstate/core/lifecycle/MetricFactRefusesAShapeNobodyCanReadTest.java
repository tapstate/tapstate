package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The internal metric fact and its invariants. Each one here is a shape that reads as a plausible
 * measurement on arrival and answers a consumer's question wrongly, so it is refused where it is
 * built rather than where it is read — the reader has no way to detect any of them.
 */
class MetricFactRefusesAShapeNobodyCanReadTest {

    private static final Instant STARTED = Instant.parse("2026-09-16T00:00:00Z");
    private static final Instant OBSERVED = Instant.parse("2026-09-16T00:05:00Z");

    @Test
    @DisplayName("an accumulating metric carries what it accumulates from, on every point")
    void aCounterPointWithoutItsStartIsRefused() {
        MetricPoint noStart = MetricPoint.reading(Map.of(), OBSERVED, 128500L);

        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER,
                "{record}", noStart))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("accumulates")
                .hasMessageContaining("went backwards");
    }

    @Test
    @DisplayName("the same metric read at a moment needs no start")
    void aGaugePointWithoutAStartIsFine() {
        MetricFact fact = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(Map.of(), OBSERVED, 12L));

        assertThat(fact.points()).singleElement()
                .satisfies(point -> assertThat(point.startTime()).isNull());
    }

    @Test
    @DisplayName("a point is a value or a distribution, never both and never neither")
    void aPointCarriesExactlyOneKindOfValue() {
        HistogramValue distribution = new HistogramValue(3, 1.5, List.of(1.0), List.of(2L, 1L));

        assertThatThrownBy(() -> new MetricPoint(Map.of(), STARTED, OBSERVED, 7L, distribution))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(() -> new MetricPoint(Map.of(), STARTED, OBSERVED, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    @DisplayName("a histogram's points carry distributions, and a counter's carry values")
    void aPointMustMatchTheKindOfMetricItBelongsTo() {
        MetricPoint plain = MetricPoint.accumulated(Map.of(), STARTED, OBSERVED, 4L);
        MetricPoint spread = MetricPoint.distribution(Map.of(), STARTED, OBSERVED,
                new HistogramValue(3, 1.5, List.of(1.0), List.of(2L, 1L)));

        assertThatThrownBy(() -> MetricFact.single("d", MetricType.HISTOGRAM, "s", plain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("carry a distribution");
        assertThatThrownBy(() -> MetricFact.single("c", MetricType.COUNTER, "{record}", spread))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("carry a single value");
    }

    @Test
    @DisplayName("one series cannot hold two values at once")
    void twoPointsWithTheSameAttributesAreRefused() {
        Map<String, String> sameSeries = Map.of("direction", "in");

        assertThatThrownBy(() -> new MetricFact("tapstate.pipeline.records", MetricType.GAUGE,
                "{record}", List.of(MetricPoint.reading(sameSeries, OBSERVED, 1L),
                        MetricPoint.reading(sameSeries, OBSERVED, 2L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two points for the same attributes");
    }

    @Test
    @DisplayName("points that differ by one attribute are two series, not a clash")
    void pointsWithDifferentAttributesCoexist() {
        MetricFact fact = new MetricFact("tapstate.pipeline.records", MetricType.GAUGE, "{record}",
                List.of(MetricPoint.reading(Map.of("direction", "in"), OBSERVED, 1L),
                        MetricPoint.reading(Map.of("direction", "out"), OBSERVED, 2L)));

        assertThat(fact.points()).hasSize(2);
    }

    @Test
    @DisplayName("a distribution carries one more count than it has bounds")
    void aHistogramWithoutItsOverflowBucketIsRefused() {
        assertThatThrownBy(() -> new HistogramValue(3, 1.5, List.of(1.0, 2.0), List.of(2L, 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow bucket");
    }

    @Test
    @DisplayName("bucket bounds ascend strictly")
    void aHistogramWhoseBoundsDoNotAscendIsRefused() {
        assertThatThrownBy(() -> new HistogramValue(3, 1.5, List.of(2.0, 2.0), List.of(1L, 1L, 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ascend");
    }

    @Test
    @DisplayName("a metric states its unit rather than leaving it to be read off its name")
    void aMetricWithoutAUnitIsRefused() {
        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.records", MetricType.GAUGE, " ",
                MetricPoint.reading(Map.of(), OBSERVED, 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unit");
    }

    @Test
    @DisplayName("a point with no time is a number nothing can be placed against")
    void aPointWithoutAnObservationTimeIsRefused() {
        assertThatThrownBy(() -> MetricPoint.reading(Map.of(), null, 1L))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("observedAt");
    }
}
