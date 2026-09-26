package io.tapstate.control.core;

import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Public best-effort event projection with no storage or execution identity. */
public record PipelineEvents(String pipelineId, Instant from, Instant to, Instant effectiveFrom,
        Instant effectiveTo, Instant retentionCutoff, Completeness completeness,
        List<Event> events, List<KnownGap> knownGaps, String nextCursor) {

    public enum Completeness { BEST_EFFORT }

    public record Failure(String code, Map<String, String> params, String message) {
        public Failure {
            params = Map.copyOf(params);
        }
    }

    public record Event(String id, Instant occurredAt, PipelineEvent.Kind kind, String message,
            PipelineState beforeState, PipelineState afterState, Failure failure, String reason) {
    }

    public record KnownGap(String eventId, Instant from, Instant to,
            List<PipelineEvent.GapReason> reasons) {
        public KnownGap {
            reasons = List.copyOf(reasons);
        }
    }

    public PipelineEvents {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(effectiveFrom, "effectiveFrom");
        Objects.requireNonNull(effectiveTo, "effectiveTo");
        Objects.requireNonNull(retentionCutoff, "retentionCutoff");
        Objects.requireNonNull(completeness, "completeness");
        events = List.copyOf(events);
        knownGaps = List.copyOf(knownGaps);
    }
}
