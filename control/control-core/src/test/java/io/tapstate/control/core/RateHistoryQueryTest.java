package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapstate.control.core.HistoryTestSupport.COUNTING_SINCE;
import static io.tapstate.control.core.HistoryTestSupport.points;
import static io.tapstate.control.core.HistoryTestSupport.query;
import static io.tapstate.control.core.HistoryTestSupport.sample;
import static io.tapstate.control.core.HistoryTestSupport.service;
import static org.assertj.core.api.Assertions.assertThat;

/** The raw walk keeps real time, reset and gap boundaries across small response pages. */
class RateHistoryQueryTest {

    @Test
    void aSmallPageWalkUsesThePredecessorAndNeverJoinsResetGapOrInboundCounters() {
        HistoryTestSupport.MutableHistory store = new HistoryTestSupport.MutableHistory();
        store.add(sample("2026-09-21T09:59:30Z", 0, 0, 1_000, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:00:30Z", 60, 600, 2_000,
                Map.of("orders", 1L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:01:30Z", 120, 1_200, 3_000,
                Map.of("orders", 2L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:01:30Z", 125, 1_250, 4_000,
                Map.of("orders", 3L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:02:30Z", 5, 50, 5_000,
                Map.of("orders", 4L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:03:30Z", 65, 650, 6_000,
                Map.of("orders", 5L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:10:30Z", 485, 4_850, 7_000,
                Map.of("orders", 6L), COUNTING_SINCE.plusSeconds(1)));

        PipelineHistoryQuery first = query("2026-09-21T10:00:00Z", "2026-09-21T10:11:00Z",
                HistoryResolution.RAW, 2, List.of("orders"), null);
        List<PipelineMetricsHistory.Point> walked = new ArrayList<>();
        List<PipelineMetricsHistory.StartReason> reasons = new ArrayList<>();
        String cursor = null;
        do {
            PipelineMetricsHistory page = service(store, 1024, 25_000).query(new PipelineHistoryQuery(
                    first.pipelineId(), first.from(), first.to(), first.resolution(), first.limit(),
                    first.tables(), cursor));
            walked.addAll(points(page));
            page.segments().forEach(segment -> reasons.add(segment.startReason()));
            assertThat(page.consistency()).isEqualTo(PipelineMetricsHistory.Consistency.EVENTUAL);
            cursor = page.nextCursor();
        } while (cursor != null);

        assertThat(walked).extracting(PipelineMetricsHistory.Point::intervalEnd).containsExactly(
                Instant.parse("2026-09-21T10:00:30Z"),
                Instant.parse("2026-09-21T10:01:30Z"),
                Instant.parse("2026-09-21T10:01:30Z"),
                Instant.parse("2026-09-21T10:02:30Z"),
                Instant.parse("2026-09-21T10:03:30Z"),
                Instant.parse("2026-09-21T10:10:30Z"));
        assertThat(reasons).containsExactly(
                PipelineMetricsHistory.StartReason.WINDOW_START,
                PipelineMetricsHistory.StartReason.CONTINUATION,
                PipelineMetricsHistory.StartReason.COUNTER_RESET,
                PipelineMetricsHistory.StartReason.CONTINUATION,
                PipelineMetricsHistory.StartReason.GAP);

        assertThat(walked.get(0).recordsOut().averageRate()).isEqualByComparingTo("1");
        assertThat(walked.get(0).bytesOut().averageRate()).isEqualByComparingTo("10");
        assertThat(walked.get(2).recordsOut()).as("same-time samples are retained but not differenced").isNull();
        assertThat(walked.get(3).recordsOut()).as("the reset boundary has no cross-run rate").isNull();
        assertThat(walked.get(4).recordsOut().averageRate()).isEqualByComparingTo("1");
        assertThat(walked.get(5).recordsOut()).as("the gap boundary has no interpolated rate").isNull();
        assertThat(walked.stream()
                .map(PipelineMetricsHistory.Point::recordsOut)
                .filter(java.util.Objects::nonNull)
                .map(PipelineMetricsHistory.Rate::averageRate))
                .allMatch(rate -> rate.signum() >= 0);

        // The input deliberately carried a much larger records.in series. The two deltas above still
        // follow the outbound values: no inbound total was given the outbound counter start.
        assertThat(walked.get(0).recordsOut().delta()).isEqualByComparingTo("60");
        assertThat(walked.get(0).bytesOut().delta()).isEqualByComparingTo("600");
        assertThat(PipelineMetricsHistory.Point.class.getRecordComponents())
                .extracting(java.lang.reflect.RecordComponent::getName)
                .containsExactly("intervalStart", "intervalEnd", "recordsOut", "bytesOut", "lag");
    }
}
