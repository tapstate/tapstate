package io.tapstate.control.core;

import io.tapstate.control.core.HistoryAggregator.Emitted;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Key;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.tapstate.control.core.HistoryTestSupport.COUNTING_SINCE;
import static org.assertj.core.api.Assertions.assertThat;

/** Partial UTC buckets use only evidenced in-window contributions, independent of input batching. */
class HistoryPartialBucketClipsToTheRequestedWindowTest {

    private static final Instant FROM = Instant.parse("2026-09-21T10:07:00Z");
    private static final Instant TO = Instant.parse("2026-09-21T10:43:00Z");

    @Test
    void resetGapAndWindowClippingKeepManualDeltasAndIgnoreOutsideLag() {
        HistoryAggregator.Projection oneBatch = project(List.of(6));
        HistoryAggregator.Projection smallBatches = project(List.of(2, 1, 3));

        assertThat(smallBatches).isEqualTo(oneBatch);
        assertThat(oneBatch.points()).extracting(Emitted::startReason).containsExactly(
                PipelineMetricsHistory.StartReason.WINDOW_START,
                PipelineMetricsHistory.StartReason.COUNTER_RESET,
                PipelineMetricsHistory.StartReason.GAP);
        assertThat(oneBatch.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.gap().intervalStart()).isEqualTo(Instant.parse("2026-09-21T10:28:00Z"));
            assertThat(gap.gap().intervalEnd()).isEqualTo(Instant.parse("2026-09-21T10:40:00Z"));
        });

        List<PipelineMetricsHistory.Point> points = oneBatch.points().stream()
                .map(Emitted::point).toList();
        assertRate(points.get(0), "220", "0.333333333", "0.333333333");
        assertThat(points.get(0).intervalStart()).isEqualTo(FROM);
        assertThat(points.get(0).lag()).singleElement().satisfies(lag -> {
            assertThat(lag.last()).isEqualTo(12L);
            assertThat(lag.max()).isEqualTo(12L);
        });

        assertRate(points.get(1), "30", "0.166666667", "0.166666667");
        assertThat(points.get(1).lag()).singleElement().satisfies(lag -> {
            assertThat(lag.last()).isEqualTo(3L);
            assertThat(lag.max()).isEqualTo(4L);
        });

        assertRate(points.get(2), "30", "0.166666667", "0.166666667");
        assertThat(points.get(2).intervalEnd()).isEqualTo(TO);
        assertThat(points.get(2).lag()).singleElement().satisfies(lag -> {
            assertThat(lag.last()).isEqualTo(6L);
            assertThat(lag.max()).isEqualTo(7L);
        });

        BigDecimal accounted = points.stream().map(PipelineMetricsHistory.Point::recordsOut)
                .map(PipelineMetricsHistory.Rate::delta).reduce(BigDecimal.ZERO, BigDecimal::add);
        assertThat(accounted).isEqualByComparingTo("280");
        assertThat(points).allSatisfy(point -> assertThat(point.lag())
                .noneSatisfy(lag -> assertThat(lag.max()).isEqualTo(99L)));
    }

    private static HistoryAggregator.Projection project(List<Integer> batchSizes) {
        HistoryAggregator aggregator = new HistoryAggregator(FROM, TO, FROM, Duration.ofMinutes(30),
                Duration.ofMinutes(5), List.of("orders"),
                PipelineMetricsHistory.StartReason.WINDOW_START, 20);
        aggregator.begin(entry(0, "2026-09-21T10:05:00Z", 0, 99, COUNTING_SINCE));
        List<Entry> samples = List.of(
                entry(1, "2026-09-21T10:10:00Z", 100, 2, COUNTING_SINCE),
                entry(2, "2026-09-21T10:18:00Z", 260, 12, COUNTING_SINCE),
                entry(3, "2026-09-21T10:25:00Z", 5, 4, COUNTING_SINCE.plusSeconds(1)),
                entry(4, "2026-09-21T10:28:00Z", 35, 3, COUNTING_SINCE.plusSeconds(1)),
                entry(5, "2026-09-21T10:40:00Z", 155, 7, COUNTING_SINCE.plusSeconds(1)),
                entry(6, "2026-09-21T10:42:00Z", 175, 6, COUNTING_SINCE.plusSeconds(1)));
        int offset = 0;
        for (int size : batchSizes) {
            for (Entry sample : samples.subList(offset, offset + size)) {
                aggregator.add(sample);
            }
            offset += size;
        }
        return aggregator.finish(entry(7, "2026-09-21T10:50:00Z", 255, 99,
                COUNTING_SINCE.plusSeconds(1)));
    }

    private static Entry entry(int key, String at, long records, long lag, Instant countingSince) {
        Instant observedAt = Instant.parse(at);
        RateSample sample = new RateSample("orders", observedAt,
                Map.of("records.out", records), Map.of("orders", lag), countingSince);
        return new Entry(new Key(observedAt, "%03d".formatted(key)), sample);
    }

    private static void assertRate(
            PipelineMetricsHistory.Point point, String delta, String average, String max) {
        assertThat(point.recordsOut().delta()).isEqualByComparingTo(delta);
        assertThat(point.recordsOut().averageRate()).isEqualByComparingTo(average);
        assertThat(point.recordsOut().maxRate()).isEqualByComparingTo(max);
    }
}
