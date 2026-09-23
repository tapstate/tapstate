package io.tapstate.cli;

import java.util.List;

/** Outcome of the shared {@code pipeline.explain} projection. */
sealed interface ExplainOutcome {

    record Found(
            String pipelineId,
            String state,
            String kind,
            String message,
            String observedAt,
            Long observedAgeMillis,
            String freshness,
            List<Evidence> evidence,
            List<String> cannotSay,
            Next next,
            Pending pending) implements ExplainOutcome {

        public Found {
            evidence = List.copyOf(evidence);
            cannotSay = List.copyOf(cannotSay);
        }
    }

    record Evidence(String source, String field, Object value) {
    }

    record Next(String action, String message) {
    }

    record Pending(String reason) {
    }

    record Rejected(String code, String message) implements ExplainOutcome {
    }

    record Unreachable() implements ExplainOutcome {
    }
}
