package io.tapstate.control.core;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** The graph-ready, reset-aware projection of one bounded pipeline history query. */
public record PipelineMetricsHistory(
        String pipelineId,
        Instant from,
        Instant to,
        Instant effectiveFrom,
        Instant effectiveTo,
        Instant retentionCutoff,
        EffectiveHistoryResolution effectiveResolution,
        Status status,
        Consistency consistency,
        List<Segment> segments,
        List<Gap> gaps,
        List<Unavailable> unavailable,
        String nextCursor) {

    public PipelineMetricsHistory {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(effectiveFrom, "effectiveFrom");
        Objects.requireNonNull(effectiveTo, "effectiveTo");
        Objects.requireNonNull(retentionCutoff, "retentionCutoff");
        Objects.requireNonNull(effectiveResolution, "effectiveResolution");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(consistency, "consistency");
        segments = List.copyOf(Objects.requireNonNull(segments, "segments"));
        gaps = List.copyOf(Objects.requireNonNull(gaps, "gaps"));
        unavailable = List.copyOf(Objects.requireNonNull(unavailable, "unavailable"));
    }

    public enum Status {
        OK,
        NO_RETAINED_SAMPLES
    }

    public enum Consistency {
        EVENTUAL
    }

    public enum StartReason {
        WINDOW_START,
        CONTINUATION,
        COUNTER_RESET,
        GAP,
        EXECUTION_CHANGE
    }

    public enum GapReason {
        SAMPLE_GAP
    }

    public record Segment(Instant intervalStart, Instant intervalEnd, StartReason startReason,
            List<Point> points) {
        public Segment {
            Objects.requireNonNull(intervalStart, "intervalStart");
            Objects.requireNonNull(intervalEnd, "intervalEnd");
            Objects.requireNonNull(startReason, "startReason");
            points = List.copyOf(Objects.requireNonNull(points, "points"));
        }
    }

    public record Point(Instant intervalStart, Instant intervalEnd, Rate recordsOut,
            Rate bytesOut, List<Lag> lag) {
        public Point {
            Objects.requireNonNull(intervalStart, "intervalStart");
            Objects.requireNonNull(intervalEnd, "intervalEnd");
            lag = List.copyOf(Objects.requireNonNull(lag, "lag"));
        }
    }

    public record Rate(BigDecimal delta, BigDecimal averageRate, BigDecimal maxRate) {
        public Rate {
            Objects.requireNonNull(delta, "delta");
            Objects.requireNonNull(averageRate, "averageRate");
            Objects.requireNonNull(maxRate, "maxRate");
        }
    }

    public record Lag(String table, Instant observedAt, long last, long max) {
        public Lag {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(observedAt, "observedAt");
        }
    }

    public record Gap(Instant intervalStart, Instant intervalEnd, GapReason reason) {
        public Gap {
            Objects.requireNonNull(intervalStart, "intervalStart");
            Objects.requireNonNull(intervalEnd, "intervalEnd");
            Objects.requireNonNull(reason, "reason");
        }
    }

    public record Unavailable(String metric, String table) {
        public Unavailable {
            Objects.requireNonNull(metric, "metric");
        }
    }
}
