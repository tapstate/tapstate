package io.tapstate.cli;

import java.util.List;

/**
 * The outcome of reading, or accepting, what a pipeline's join steps derive their output columns to be
 * ({@code GET /api/pipelines/{id}/derived-schema}, {@code POST /api/pipelines/{id}:accept-derived-schema}).
 * Either the server answered with a report per derived step, or it refused with a coded reason, or it
 * could not be reached. Sealed so the caller renders each branch without try/catch, mirroring the
 * never-throw transport seam.
 */
sealed interface DerivedSchemaOutcome {

    /** The server answered; one report per derived step, empty for a pipeline with no join. */
    record Found(String pipelineId, List<RemoteDerivedStep> steps) implements DerivedSchemaOutcome {

        public Found {
            steps = steps == null ? List.of() : List.copyOf(steps);
        }
    }

    /** The server refused with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements DerivedSchemaOutcome {
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements DerivedSchemaOutcome {
    }
}
