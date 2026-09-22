package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** A raw cursor is stateless and query-bound, while its multi-request view stays explicitly eventual. */
class RawHistoryCursorIsBoundedAndEventualTest {

    @Test
    void aCursorDoesNotRevisitLateKeysOrPromiseSamplesThatRetentionRemoved() {
        HistoryTestSupport.MutableClock clock = new HistoryTestSupport.MutableClock(
                Instant.parse("2026-09-21T12:00:00Z"));
        HistoryTestSupport.MutableHistory store = new HistoryTestSupport.MutableHistory();
        store.add(sample("2026-09-21T11:49:00Z", 0, 0, 0, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T11:51:00Z", 120, 1_200, 1_000, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T11:52:00Z", 180, 1_800, 2_000, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T11:53:00Z", 240, 2_400, 3_000, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T11:54:00Z", 300, 3_000, 4_000, Map.of(), COUNTING_SINCE));

        PipelineHistoryQuery request = query("2026-09-21T11:50:00Z", "2026-09-21T12:10:00Z",
                HistoryResolution.RAW, 2, List.of(), null);
        PipelineMetricsHistory first = service(store, 1024, 25_000, clock).query(request);
        assertThat(first.nextCursor()).isNotBlank();
        assertThat(first.effectiveTo()).isEqualTo(clock.instant());

        TapstateException changed = catchThrowableOfType(() -> service(store, 1024, 25_000, clock).query(
                new PipelineHistoryQuery(request.pipelineId(), request.from(), request.to(),
                        request.resolution(), request.limit(), List.of("orders"), first.nextCursor())),
                TapstateException.class);
        assertThat(changed.code()).isEqualTo(MonitorError.INVALID_CURSOR);
        assertThat(changed.args()).containsEntry("reason", "QUERY_MISMATCH");

        char replacement = first.nextCursor().endsWith("A") ? 'B' : 'A';
        String tampered = first.nextCursor().substring(0, first.nextCursor().length() - 1) + replacement;
        TapstateException altered = catchThrowableOfType(() -> service(store, 1024, 25_000, clock).query(
                new PipelineHistoryQuery(request.pipelineId(), request.from(), request.to(),
                        request.resolution(), request.limit(), request.tables(), tampered)),
                TapstateException.class);
        assertThat(altered.code()).isEqualTo(MonitorError.INVALID_CURSOR);

        store.add(sample("2026-09-21T11:51:30Z", 150, 1_500, 1_500, Map.of(), COUNTING_SINCE));
        store.removeAt(Instant.parse("2026-09-21T11:53:00Z"));
        clock.advance(Duration.ofMinutes(1));

        // A fresh service instance accepts the token: continuation state lives in the signed cursor,
        // not in a server-side session. Its newer clock must not move the frozen first-page bounds.
        PipelineMetricsHistory second = service(store, 1024, 25_000, clock).query(
                new PipelineHistoryQuery(request.pipelineId(), request.from(), request.to(),
                        request.resolution(), request.limit(), request.tables(), first.nextCursor()));
        assertThat(second.effectiveTo()).isEqualTo(first.effectiveTo());
        assertThat(second.retentionCutoff()).isEqualTo(first.retentionCutoff());
        assertThat(second.consistency()).isEqualTo(PipelineMetricsHistory.Consistency.EVENTUAL);
        assertThat(points(second)).extracting(PipelineMetricsHistory.Point::intervalEnd)
                .containsExactly(Instant.parse("2026-09-21T11:54:00Z"));

        List<Instant> walked = new ArrayList<>();
        walked.addAll(points(first).stream().map(PipelineMetricsHistory.Point::intervalEnd).toList());
        walked.addAll(points(second).stream().map(PipelineMetricsHistory.Point::intervalEnd).toList());
        assertThat(walked).containsExactly(
                Instant.parse("2026-09-21T11:51:00Z"),
                Instant.parse("2026-09-21T11:52:00Z"),
                Instant.parse("2026-09-21T11:54:00Z"));

        PipelineMetricsHistory refreshed = service(store, 1024, 25_000, clock).query(
                new PipelineHistoryQuery(request.pipelineId(), request.from(), request.to(),
                        request.resolution(), 10, request.tables(), null));
        assertThat(refreshed.effectiveTo()).isEqualTo(clock.instant());
        assertThat(points(refreshed)).extracting(PipelineMetricsHistory.Point::intervalEnd)
                .containsExactly(
                        Instant.parse("2026-09-21T11:51:00Z"),
                        Instant.parse("2026-09-21T11:51:30Z"),
                        Instant.parse("2026-09-21T11:52:00Z"),
                        Instant.parse("2026-09-21T11:54:00Z"));

        clock.advance(Duration.ofMinutes(9));
        TapstateException expired = catchThrowableOfType(() -> service(store, 1024, 25_000, clock).query(
                new PipelineHistoryQuery(request.pipelineId(), request.from(), request.to(),
                        request.resolution(), request.limit(), request.tables(), first.nextCursor())),
                TapstateException.class);
        assertThat(expired.code()).isEqualTo(MonitorError.CURSOR_EXPIRED);
    }
}
