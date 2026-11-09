package io.tapstate.cli;

import java.util.Map;

/**
 * The outcome of a nest explain read ({@code GET /api/pipelines/{id}/nest/{step}/explain}). Either the
 * server answered with the compiled tree, the samples and the warnings with their text already rendered, or
 * it refused with a coded reason, or it could not be reached. Sealed so the caller renders each branch without try/catch.
 */
sealed interface NestExplainOutcome {

    /** The server answered with the view, kept as the decoded tree: it is printed, never computed on. */
    record Found(Map<?, ?> view) implements NestExplainOutcome {
    }

    /** The server refused with a coded reason already rendered to a message. */
    record Rejected(String code, String message, Map<String, Object> params) implements NestExplainOutcome {

        public Rejected {
            params = params == null ? Map.of() : Map.copyOf(params);
        }

        public Rejected(String code, String message) {
            this(code, message, Map.of());
        }
    }

    /** The server could not be reached (connection refused, timeout, or a malformed answer). */
    record Unreachable() implements NestExplainOutcome {
    }
}
