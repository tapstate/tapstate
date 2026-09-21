package io.tapstate.control.core;

import java.math.BigDecimal;
import java.math.RoundingMode;
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

    private static final int WIRE_SCALE = 9;

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
        GAP
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
            delta = wireNumber(Objects.requireNonNull(delta, "delta"));
            averageRate = wireNumber(Objects.requireNonNull(averageRate, "averageRate"));
            maxRate = wireNumber(Objects.requireNonNull(maxRate, "maxRate"));
        }
    }

    private static BigDecimal wireNumber(BigDecimal value) {
        BigDecimal rounded = value.setScale(WIRE_SCALE, RoundingMode.HALF_EVEN).stripTrailingZeros();
        if (rounded.signum() == 0) {
            return BigDecimal.ZERO;
        }
        return rounded.scale() < 0 ? rounded.setScale(0) : rounded;
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
