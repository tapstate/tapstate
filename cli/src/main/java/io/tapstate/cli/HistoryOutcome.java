package io.tapstate.cli;

import java.util.List;

/** Outcome of a typed {@code pipeline.metrics.history} read. */
sealed interface HistoryOutcome {

    record Found(
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
            String nextCursor) implements HistoryOutcome {

        public Found {
            segments = List.copyOf(segments);
            gaps = List.copyOf(gaps);
            unavailable = List.copyOf(unavailable);
        }
    }

    record Segment(String intervalStart, String intervalEnd, String startReason, List<Point> points) {
        public Segment {
            points = List.copyOf(points);
        }
    }

    record Point(String intervalStart, String intervalEnd, Rate recordsOut, Rate bytesOut, List<Lag> lag) {
        public Point {
            lag = List.copyOf(lag);
        }
    }

    record Rate(Number delta, Number averageRate, Number maxRate) {
    }

    record Lag(String table, String observedAt, long last, long max) {
    }

    record Gap(String intervalStart, String intervalEnd, String reason) {
    }

    record Unavailable(String metric, String table) {
    }

    record Rejected(String code, String message) implements HistoryOutcome {
    }

    record Unreachable() implements HistoryOutcome {
    }
}
