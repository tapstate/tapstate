package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bucket bounds the two duration histograms publish with, as the design document lists them. The
 * numbers here are a copy of that document's table on purpose: a bound changed in the code without the
 * document, or in the document without the code, reddens this rather than shipping two layouts.
 *
 * <p>The rest of these are the refusals that make the bounds a rule rather than a suggestion: a
 * distribution built over any other bounds, under a name with none, or in another unit than the bounds
 * are given in, is refused where its fact is built.
 */
class WhichBucketsADurationFallsIntoTest {

    private static final Instant STARTED = Instant.parse("2026-09-17T00:00:00Z");
    private static final Instant OBSERVED = Instant.parse("2026-09-17T00:05:00Z");

    /** A distribution over {@code bounds} with {@code count} observations, all in the first bucket. */
    private static HistogramValue observations(HistogramBounds bounds, long count) {
        List<Long> counts = new ArrayList<>(Collections.nCopies(bounds.buckets(), 0L));
        counts.set(0, count);
        return bounds.value(count, count * 0.005, counts);
    }

    @Test
    @DisplayName("a row's delivery is bucketed from ten milliseconds to an hour")
    void deliveryBoundsAreTheDocumentedOnes() {
        assertThat(HistogramBounds.RECORD_DELIVERY_DURATION.bounds()).containsExactly(
                0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0);
        assertThat(HistogramBounds.RECORD_DELIVERY_DURATION.buckets()).isEqualTo(16);
        assertThat(HistogramBounds.RECORD_DELIVERY_DURATION.instrument())
                .isEqualTo("tapstate.pipeline.record.delivery.duration");
    }

    @Test
    @DisplayName("a stage's work is bucketed from a tenth of a millisecond to ten seconds")
    void processBoundsAreTheDocumentedOnes() {
        assertThat(HistogramBounds.PROCESS_DURATION.bounds()).containsExactly(
                0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
                5.0, 10.0);
        assertThat(HistogramBounds.PROCESS_DURATION.buckets()).isEqualTo(17);
        assertThat(HistogramBounds.PROCESS_DURATION.instrument())
                .isEqualTo("tapstate.pipeline.process.duration");
    }

    @Test
    @DisplayName("exactly the two histogram instruments have bounds, and both are in seconds")
    void onlyTheTwoHistogramInstrumentsHaveBounds() {
        assertThat(Arrays.stream(HistogramBounds.values()).map(HistogramBounds::instrument))
                .containsExactlyInAnyOrder("tapstate.pipeline.record.delivery.duration",
                        "tapstate.pipeline.process.duration");
        assertThat(HistogramBounds.UNIT).isEqualTo("s");
        assertThat(HistogramBounds.forInstrument("tapstate.pipeline.lag")).isEmpty();
    }

    @Test
    @DisplayName("a distribution over the registered bounds builds, under its own name and unit")
    void aDistributionOverTheRegisteredBoundsBuilds() {
        MetricFact fact = MetricFact.single("tapstate.pipeline.record.delivery.duration",
                MetricType.HISTOGRAM, "s", MetricPoint.distribution(
                        Map.of("tapstate.table.id", "orders"), STARTED, OBSERVED,
                        observations(HistogramBounds.RECORD_DELIVERY_DURATION, 3)));

        assertThat(fact.points()).singleElement()
                .satisfies(point -> assertThat(point.histogram().bucketCounts()).hasSize(16));
    }

    @Test
    @DisplayName("a distribution over any other bounds is refused where its fact is built")
    void aDistributionOverUnregisteredBoundsIsRefused() {
        HistogramValue libraryDefault = new HistogramValue(3, 1.5, List.of(1.0), List.of(2L, 1L));

        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.record.delivery.duration",
                MetricType.HISTOGRAM, "s", MetricPoint.distribution(Map.of(), STARTED, OBSERVED,
                        libraryDefault)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("registered bounds")
                .hasMessageContaining("[1.0]");
    }

    @Test
    @DisplayName("a distribution under a name that registered no bounds is refused")
    void aDistributionUnderAnUnregisteredNameIsRefused() {
        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.commit.duration",
                MetricType.HISTOGRAM, "s", MetricPoint.distribution(Map.of(), STARTED, OBSERVED,
                        observations(HistogramBounds.PROCESS_DURATION, 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no bucket bounds are registered");
    }

    @Test
    @DisplayName("a distribution in another unit than its bounds are given in is refused")
    void aDistributionInTheWrongUnitIsRefused() {
        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.process.duration",
                MetricType.HISTOGRAM, "ms", MetricPoint.distribution(Map.of(), STARTED, OBSERVED,
                        observations(HistogramBounds.PROCESS_DURATION, 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'ms'")
                .hasMessageContaining("own unit");
    }

    @Test
    @DisplayName("the registered bounds build a distribution of their own width and refuse another")
    void theRegisteredBoundsBuildTheirOwnWidth() {
        HistogramBounds bounds = HistogramBounds.RECORD_DELIVERY_DURATION;

        assertThat(bounds.value(0, 0.0, Collections.nCopies(16, 0L)).bucketCounts()).hasSize(16);
        assertThatThrownBy(() -> bounds.value(0, 0.0, Collections.nCopies(15, 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow bucket");
    }
}
