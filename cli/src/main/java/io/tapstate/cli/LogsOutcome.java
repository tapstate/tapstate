package io.tapstate.cli;

import java.util.List;

/**
 * The outcome of a remote pipeline logs read ({@code GET /api/pipelines/{id}/logs}). Either the read found
 * the pipeline's recent log lines (oldest to newest), or it was refused with a coded reason, or the server
 * could not be reached. The list is empty when the pipeline has logged nothing on the served node — a benign
 * empty tail, not an error. Sealed so the caller renders each branch without try/catch, mirroring the
 * never-throw transport seam.
 */
sealed interface LogsOutcome {

    /** The read found the pipeline's recent log lines, oldest to newest; empty when it has logged nothing. */
    record Found(String pipelineId, List<RemoteLogLine> lines, RemoteLogCursor nextCursor, boolean truncated)
            implements LogsOutcome {

        Found(String pipelineId, List<RemoteLogLine> lines) {
            this(pipelineId, lines, null, false);
        }

        public Found {
            lines = lines == null ? List.of() : List.copyOf(lines);
        }
    }

    /** The server refused the read with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements LogsOutcome {
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements LogsOutcome {
    }
}
