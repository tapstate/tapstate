package io.tapstate.cli;

/**
 * The outcome of a remote pipeline status read ({@code GET /api/pipelines/{id}/status}). Either the read
 * found the pipeline's latest published lifecycle state, or it was refused with a coded reason (a pipeline
 * that has published no observation is {@code monitor.no-observation}), or the server could not be reached.
 * Sealed so the caller renders each branch without try/catch, mirroring the never-throw transport seam. The
 * CLI carries no shared control type (rule R6: it reaches the server over HTTP only), so the state travels
 * as its wire string.
 */
sealed interface StatusOutcome {

    /**
     * The read found the pipeline's lifecycle state (its wire name, e.g. {@code RUNNING}), the coded reason
     * its run died when the server published one ({@code failureCode} null while it is healthy), and how
     * long ago the reading behind all of it was taken.
     * The reason arrives as a code plus the message the server rendered for it — the same pair a coded
     * refusal carries, so both print through one renderer here.
     *
     * @param observedAgeMillis how long ago the observation this answer comes from was taken, or null when
     *                          nobody can say — an observation stored before the time was recorded, or a
     *                          server too old to report it. Null is the answer "not known" and is never
     *                          filled in from the clock on this machine: the whole point of the reading is
     *                          that a run whose publisher stopped and a run whose state has not changed
     *                          look identical from here, and a locally invented age would make them
     *                          identical again. Measured on the server because the caller's wall clock is
     *                          its own
     */
    record Found(String pipelineId, String state, String failureCode, String failureMessage,
            Long observedAgeMillis) implements StatusOutcome {

        /** A reading from a server that did not say how old it is — the shape before the time was carried. */
        Found(String pipelineId, String state, String failureCode, String failureMessage) {
            this(pipelineId, state, failureCode, failureMessage, null);
        }

        /** A status with nothing wrong to report. */
        Found(String pipelineId, String state) {
            this(pipelineId, state, null, null, null);
        }
    }

    /** The server refused the read with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements StatusOutcome {
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements StatusOutcome {
    }
}
