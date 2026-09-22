package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapstate.control.core.HistoryTestSupport.COUNTING_SINCE;
import static io.tapstate.control.core.HistoryTestSupport.NOW;
import static io.tapstate.control.core.HistoryTestSupport.points;
import static io.tapstate.control.core.HistoryTestSupport.query;
import static io.tapstate.control.core.HistoryTestSupport.sample;
import static io.tapstate.control.core.HistoryTestSupport.service;
import static org.assertj.core.api.Assertions.assertThat;

/** Resolution choice and aggregation keep stable UTC buckets without crossing reset or gap boundaries. */
class HistoryResolutionAggregationTest {

    @Test
    void autoAndEveryExplicitResolutionHaveOneStableEffectiveValue() {
        HistoryTestSupport.MutableHistory empty = new HistoryTestSupport.MutableHistory();
        Map<Duration, EffectiveHistoryResolution> auto = new LinkedHashMap<>();
        auto.put(Duration.ofHours(1), EffectiveHistoryResolution.PT1M);
        auto.put(Duration.ofHours(6), EffectiveHistoryResolution.PT5M);
        auto.put(Duration.ofDays(1), EffectiveHistoryResolution.PT30M);
        auto.put(Duration.ofDays(3), EffectiveHistoryResolution.PT1H);
        auto.put(Duration.ofDays(7), EffectiveHistoryResolution.PT3H);
        auto.put(Duration.ofDays(15), EffectiveHistoryResolution.PT6H);

        auto.forEach((span, expected) -> assertThat(service(empty, 1024, 25_000).query(
                new PipelineHistoryQuery("orders", NOW.minus(span), NOW, HistoryResolution.AUTO,
                        PipelineHistoryQuery.DEFAULT_LIMIT, List.of(), null)).effectiveResolution())
                .as("auto resolution for %s", span)
                .isEqualTo(expected));

        Map<HistoryResolution, EffectiveHistoryResolution> explicit = Map.of(
                HistoryResolution.RAW, EffectiveHistoryResolution.PT1M,
                HistoryResolution.PT5M, EffectiveHistoryResolution.PT5M,
                HistoryResolution.PT30M, EffectiveHistoryResolution.PT30M,
                HistoryResolution.PT1H, EffectiveHistoryResolution.PT1H,
                HistoryResolution.PT3H, EffectiveHistoryResolution.PT3H,
                HistoryResolution.PT6H, EffectiveHistoryResolution.PT6H);
        explicit.forEach((requested, expected) -> assertThat(service(empty, 1024, 25_000).query(
                new PipelineHistoryQuery("orders", NOW.minus(Duration.ofHours(1)), NOW, requested,
                        PipelineHistoryQuery.DEFAULT_LIMIT, List.of(), null)).effectiveResolution())
                .as("explicit resolution %s", requested)
                .isEqualTo(expected));
    }

    @Test
    void oneUtcBucketRetainsDeltaRateAndLagPeaksThenSplitsAtResetAndGap() {
        HistoryTestSupport.MutableHistory store = new HistoryTestSupport.MutableHistory();
        store.add(sample("2026-09-21T09:59:00Z", 0, 0, 0, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:00:00Z", 60, 600, 0,
                Map.of("orders", 9L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:01:00Z", 120, 1_200, 0,
                Map.of("orders", 2L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:02:00Z", 240, 2_400, 0,
                Map.of("orders", 4L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:03:00Z", 5, 50, 0,
                Map.of("orders", 1L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:04:00Z", 65, 650, 0,
                Map.of("orders", 3L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:10:00Z", 425, 4_250, 0,
                Map.of("orders", 7L), COUNTING_SINCE.plusSeconds(1)));

        PipelineMetricsHistory answer = service(store, 2, 25_000).query(
                query("2026-09-21T10:00:00Z", "2026-09-21T10:11:00Z",
                        HistoryResolution.PT30M, 20, List.of("orders"), null));

        assertThat(answer.effectiveResolution()).isEqualTo(EffectiveHistoryResolution.PT30M);
        assertThat(answer.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(
                        PipelineMetricsHistory.StartReason.WINDOW_START,
                        PipelineMetricsHistory.StartReason.COUNTER_RESET,
                        PipelineMetricsHistory.StartReason.GAP);
        assertThat(answer.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart().toString()).isEqualTo("2026-09-21T10:04:00Z");
            assertThat(gap.intervalEnd().toString()).isEqualTo("2026-09-21T10:10:00Z");
        });

        List<PipelineMetricsHistory.Point> points = points(answer);
        assertThat(points.get(0).recordsOut().delta()).isEqualByComparingTo("180");
        assertThat(points.get(0).recordsOut().averageRate()).isEqualByComparingTo("1.5");
        assertThat(points.get(0).recordsOut().maxRate()).isEqualByComparingTo("2");
        assertThat(points.get(0).lag()).singleElement().satisfies(lag -> {
            assertThat(lag.last()).isEqualTo(4L);
            assertThat(lag.max()).isEqualTo(9L);
        });
        assertThat(points.get(1).recordsOut().delta()).isEqualByComparingTo("60");
        assertThat(points.get(2).recordsOut()).isNull();
    }
}
