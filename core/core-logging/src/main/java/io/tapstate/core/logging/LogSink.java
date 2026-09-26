package io.tapstate.core.logging;

import java.util.List;

/**
 * A node-local, in-process buffer of recent log lines keyed by pipeline id. The pipeline runtime
 * appends lines as it emits them (attributed to a pipeline); a control read face tails the most
 * recent lines for one pipeline. This is not a persistent log store: it holds only a bounded window
 * of the latest lines in memory and never leaves the process. A tail of a pipeline that has logged
 * nothing is a benign empty result, never an error.
 */
public interface LogSink {

    /** Internal owner of a captured line; never projected in a public logs response. */
    record Scope(String pipelineIncarnationId, long executionGeneration) {
        public Scope {
            java.util.Objects.requireNonNull(pipelineIncarnationId, "pipelineIncarnationId");
            if (pipelineIncarnationId.isBlank() || executionGeneration < 1) {
                throw new IllegalArgumentException("log scope requires an incarnation and execution generation");
            }
        }
    }

    /** Records one log line against a pipeline. */
    void append(String pipelineId, LogLine line);

    /** Records a line under the execution that emitted it. */
    default void append(String pipelineId, Scope scope, LogLine line) {
        throw new UnsupportedOperationException("scoped log append is unavailable");
    }

    /** Returns legacy unscoped lines, oldest to newest; only callers with a legacy artifact use this view. */
    List<LogLine> tail(String pipelineId);

    /** Reads only lines from one current execution. */
    default List<LogLine> tail(String pipelineId, Scope scope) {
        throw new UnsupportedOperationException("scoped log tail is unavailable");
    }

    /** Reads retained executions belonging to one current resource. */
    default List<LogLine> tailIncarnation(String pipelineId, String incarnationId) {
        throw new UnsupportedOperationException("incarnation log tail is unavailable");
    }

    /** Best-effort removal that cannot match a recreated resource's lines. */
    default void clearIncarnation(String pipelineId, String incarnationId) {
        throw new UnsupportedOperationException("scoped log cleanup is unavailable");
    }

    /** Removes only lines emitted before this pipeline had an identity. */
    default void clearLegacy(String pipelineId) {
        throw new UnsupportedOperationException("legacy log cleanup is unavailable");
    }
}
