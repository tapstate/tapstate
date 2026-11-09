package io.tapstate.cli;

import java.util.Map;

import java.util.List;

/**
 * The outcome of a nest progress read ({@code GET /api/pipelines/{id}/nest/progress}). Either the server
 * answered with one entry per nest step, or it refused with a coded reason (a pipeline with no nest step, or
 * a step that is not one), or it could not be reached. Sealed so the caller renders each branch without
 * try/catch, mirroring the never-throw transport seam.
 */
sealed interface NestProgressOutcome {

    /** The server answered: the pipeline's state and one entry per nest step asked about. */
    record Found(String pipelineId, String state, String phase, List<Step> steps) implements NestProgressOutcome {

        public Found {
            steps = steps == null ? List.of() : List.copyOf(steps);
        }
    }

    /** The server refused with a coded reason already rendered to a message. */
    record Rejected(String code, String message, Map<String, Object> params) implements NestProgressOutcome {

        public Rejected {
            params = params == null ? Map.of() : Map.copyOf(params);
        }

        public Rejected(String code, String message) {
            this(code, message, Map.of());
        }
    }

    /** The server could not be reached (connection refused, timeout, or a malformed answer). */
    record Unreachable() implements NestProgressOutcome {
    }

    /** One nest step, as the server reported it; numbers it did not report are null. */
    record Step(String step, String rootTable, Long rootsRendered, Long rootsEstimate, String activity,
            List<String> waitingFor, List<Table> tables) {

        public Step {
            waitingFor = waitingFor == null ? List.of() : List.copyOf(waitingFor);
            tables = tables == null ? List.of() : List.copyOf(tables);
        }
    }

    /** One table a step depends on; a null stage is a table the pipeline reported no load for. */
    record Table(String table, String path, String stage, Long rowsRead, Long rowsEstimate, Long rowsStaged) {
    }
}
