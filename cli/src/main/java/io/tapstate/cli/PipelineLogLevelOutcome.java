package io.tapstate.cli;

/** Outcome of setting a Pipeline's future node-local log capture threshold. */
sealed interface PipelineLogLevelOutcome {
    record Changed(String level) implements PipelineLogLevelOutcome {}
    record Rejected(String code, String message) implements PipelineLogLevelOutcome {}
    record Unreachable() implements PipelineLogLevelOutcome {}
}
