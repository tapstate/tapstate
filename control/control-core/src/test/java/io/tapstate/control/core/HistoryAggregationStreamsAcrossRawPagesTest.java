package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.tapstate.control.core.HistoryTestSupport.COUNTING_SINCE;
import static io.tapstate.control.core.HistoryTestSupport.query;
import static io.tapstate.control.core.HistoryTestSupport.sample;
import static io.tapstate.control.core.HistoryTestSupport.service;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Internal raw pages never become public bucket boundaries or unbounded in-memory materialization. */
class HistoryAggregationStreamsAcrossRawPagesTest {

    @Test
    void batchesOfTwoSevenAndTheNormalSizeProduceTheSameResetAndGapProjection() {
        HistoryTestSupport.MutableHistory byTwo = samples();
        HistoryTestSupport.MutableHistory bySeven = samples();
        HistoryTestSupport.MutableHistory normal = samples();
        PipelineHistoryQuery request = query("2026-09-21T06:00:00Z", "2026-09-21T12:00:00Z",
                HistoryResolution.AUTO, PipelineHistoryQuery.DEFAULT_LIMIT, List.of("orders"), null);

        PipelineHistoryQueryService.QueryRun two = service(byTwo, 2, 25_000).execute(request);
        PipelineHistoryQueryService.QueryRun seven = service(bySeven, 7, 25_000).execute(request);
        PipelineHistoryQueryService.QueryRun ordinary = service(normal, 1024, 25_000).execute(request);

        assertThat(two.history()).isEqualTo(seven.history()).isEqualTo(ordinary.history());
        assertThat(two.history().effectiveResolution()).isEqualTo(EffectiveHistoryResolution.PT5M);
        assertThat(two.history().nextCursor()).isNull();
        assertThat(two.history().segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .contains(PipelineMetricsHistory.StartReason.COUNTER_RESET,
                        PipelineMetricsHistory.StartReason.GAP);
        assertThat(two.cost().peakRawEntriesHeld()).isLessThanOrEqualTo(3);
        assertThat(seven.cost().peakRawEntriesHeld()).isLessThanOrEqualTo(8);
        assertThat(ordinary.cost().peakRawEntriesHeld()).isLessThanOrEqualTo(1025);
        assertThat(byTwo.requestedLimits()).containsOnly(2);
        assertThat(bySeven.requestedLimits()).containsOnly(7);
        assertThat(normal.requestedLimits()).containsOnly(1024);
    }

    @Test
    void cumulativeScanBudgetRefusesBeforePublishingAnIncompleteBucket() {
        HistoryTestSupport.MutableHistory dense = new HistoryTestSupport.MutableHistory();
        for (int i = 0; i < 10; i++) {
            dense.add(sample("2026-09-21T10:00:00Z", i, i * 10L, i * 100L,
                    Map.of(), COUNTING_SINCE));
        }
        PipelineHistoryQuery request = query("2026-09-21T10:00:00Z", "2026-09-21T11:00:00Z",
                HistoryResolution.PT30M, 10, List.of(), null);

        TapstateException refusal = catchThrowableOfType(
                () -> service(dense, 2, 5).query(request), TapstateException.class);

        assertThat(refusal.code()).isEqualTo(MonitorError.QUERY_BUDGET_EXCEEDED);
        assertThat(refusal.args()).containsEntry("budget", "RAW_SCAN").containsEntry("limit", 5);
    }

    private static HistoryTestSupport.MutableHistory samples() {
        HistoryTestSupport.MutableHistory store = new HistoryTestSupport.MutableHistory();
        store.add(sample("2026-09-21T09:59:00Z", 0, 0, 0, Map.of(), COUNTING_SINCE));
        for (int minute = 0; minute < 14; minute++) {
            boolean afterReset = minute >= 7;
            long records = afterReset ? 5 + (minute - 7L) * 60L : (minute + 1L) * 60L;
            long bytes = records * 10L;
            Instant start = afterReset ? COUNTING_SINCE.plusSeconds(1) : COUNTING_SINCE;
            store.add(sample("2026-09-21T10:%02d:00Z".formatted(minute), records, bytes,
                    records * 99L, Map.of("orders", (long) (minute % 5)), start));
        }
        store.add(sample("2026-09-21T10:20:00Z", 785, 7_850, 90_000,
                Map.of("orders", 7L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:21:00Z", 845, 8_450, 91_000,
                Map.of("orders", 2L), COUNTING_SINCE.plusSeconds(1)));
        return store;
    }
}
