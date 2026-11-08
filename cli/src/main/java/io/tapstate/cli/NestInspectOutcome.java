package io.tapstate.cli;

import java.util.Map;

/**
 * The outcome of a nest inspect read ({@code GET /api/pipelines/{id}/nest/{step}/documents?key=...}). Either
 * the server answered with the rendered view of what the step holds under the key, or it refused with a coded
 * reason, or it could not be reached. Sealed so the caller renders each branch without try/catch.
 */
sealed interface NestInspectOutcome {

    /** The server answered with the view, kept as the decoded tree: it is printed, never computed on. */
    record Found(Map<?, ?> view) implements NestInspectOutcome {
    }

    /** The server refused with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements NestInspectOutcome {
    }

    /** The server could not be reached (connection refused, timeout, or a malformed answer). */
    record Unreachable() implements NestInspectOutcome {
    }
}
