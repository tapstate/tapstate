package io.tapstate.cli;

import java.util.List;
import java.util.Map;

/** Typed outcome of the shared pipeline event read. */
sealed interface EventsOutcome {

    record Found(String pipelineId, String from, String to, String effectiveFrom, String effectiveTo,
            String retentionCutoff, String completeness, List<Event> events,
            List<KnownGap> knownGaps, String nextCursor) implements EventsOutcome {
        public Found {
            events = List.copyOf(events);
            knownGaps = List.copyOf(knownGaps);
        }
    }

    record Event(String id, String occurredAt, String kind, String message,
            String beforeState, String afterState, Failure failure, String reason) {
    }

    record Failure(String code, Map<String, String> params, String message) {
        public Failure {
            params = Map.copyOf(params);
        }
    }

    record KnownGap(String eventId, String from, String to, List<String> reasons) {
        public KnownGap {
            reasons = List.copyOf(reasons);
        }
    }

    record Rejected(String code, String message) implements EventsOutcome {
    }

    record Unreachable() implements EventsOutcome {
    }
}
