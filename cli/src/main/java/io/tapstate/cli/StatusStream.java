package io.tapstate.cli;

/**
 * A sink for a status watch stream: called once per state the server streams — the current state, then
 * each change. The CLI carries no shared control type (rule R6), so the state travels as its wire string.
 */
@FunctionalInterface
interface StatusStream {

    /**
     * The pipeline's current lifecycle state (its wire name, e.g. {@code RUNNING}), and the coded reason
     * its run died when the frame carried one ({@code failureCode} null while it is healthy) — the same
     * pair the one-shot status read passes to its renderer, so a watcher learns why a pipeline died from
     * the very frame that reports it dead, not only from a separate poll.
     */
    void state(String pipelineId, String state, String failureCode, String failureMessage);

    /** Retains the complete frame for consumers that render captured runtime details. */
    default void state(StatusOutcome.Found found) {
        state(found.pipelineId(), found.state(), found.failureCode(), found.failureMessage());
    }
}
