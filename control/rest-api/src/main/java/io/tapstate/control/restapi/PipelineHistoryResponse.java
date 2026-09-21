package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineMetricsHistory;

import java.math.BigDecimal;
import java.util.List;

/** Stable JSON projection of one bounded pipeline history page. */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineHistoryResponse(
        String pipelineId,
        String from,
        String to,
        String effectiveFrom,
        String effectiveTo,
        String retentionCutoff,
        String effectiveResolution,
        String status,
        String consistency,
        List<Segment> segments,
        List<Gap> gaps,
        List<Unavailable> unavailable,
        @JsonInclude(JsonInclude.Include.ALWAYS) String nextCursor) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Segment(String intervalStart, String intervalEnd, String startReason, List<Point> points) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Point(String intervalStart, String intervalEnd, Rate recordsOut, Rate bytesOut, List<Lag> lag) {
    }

    record Rate(BigDecimal delta, BigDecimal averageRate, BigDecimal maxRate) {
    }

    record Lag(String table, String observedAt, long last, long max) {
    }

    record Gap(String intervalStart, String intervalEnd, String reason) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Unavailable(String metric, String table) {
    }

    static PipelineHistoryResponse of(PipelineMetricsHistory history) {
        return new PipelineHistoryResponse(
                history.pipelineId(),
                history.from().toString(),
                history.to().toString(),
                history.effectiveFrom().toString(),
                history.effectiveTo().toString(),
                history.retentionCutoff().toString(),
                history.effectiveResolution().name(),
                history.status().name(),
                history.consistency().name(),
                history.segments().stream().map(PipelineHistoryResponse::segment).toList(),
                history.gaps().stream().map(PipelineHistoryResponse::gap).toList(),
                history.unavailable().stream().map(PipelineHistoryResponse::unavailable).toList(),
                history.nextCursor());
    }

    private static Segment segment(PipelineMetricsHistory.Segment segment) {
        return new Segment(segment.intervalStart().toString(), segment.intervalEnd().toString(),
                segment.startReason().name(), segment.points().stream().map(PipelineHistoryResponse::point).toList());
    }

    private static Point point(PipelineMetricsHistory.Point point) {
        return new Point(point.intervalStart().toString(), point.intervalEnd().toString(),
                rate(point.recordsOut()), rate(point.bytesOut()),
                point.lag().stream().map(PipelineHistoryResponse::lag).toList());
    }

    private static Rate rate(PipelineMetricsHistory.Rate rate) {
        return rate == null ? null : new Rate(rate.delta(), rate.averageRate(), rate.maxRate());
    }

    private static Lag lag(PipelineMetricsHistory.Lag lag) {
        return new Lag(lag.table(), lag.observedAt().toString(), lag.last(), lag.max());
    }

    private static Gap gap(PipelineMetricsHistory.Gap gap) {
        return new Gap(gap.intervalStart().toString(), gap.intervalEnd().toString(), gap.reason().name());
    }

    private static Unavailable unavailable(PipelineMetricsHistory.Unavailable unavailable) {
        return new Unavailable(unavailable.metric(), unavailable.table());
    }
}
