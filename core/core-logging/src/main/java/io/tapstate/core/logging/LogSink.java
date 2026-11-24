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

    /** Records one log line against a pipeline. */
    void append(String pipelineId, LogLine line);

    /** Records a line carrying the physical state generation captured by its producer. */
    default void append(String pipelineId, long generation, LogLine line) {
        append(pipelineId, line);
    }

    /**
     * Returns the most recent buffered lines for a pipeline, oldest to newest, as an immutable
     * snapshot. Empty when the pipeline has logged nothing (or is unknown to this node).
     */
    List<LogLine> tail(String pipelineId);

    /** Returns only lines in the verified lifetime window; an unscoped sink cannot answer this read. */
    default List<LogLine> tail(String pipelineId, long firstGeneration, long lastGeneration) {
        return List.of();
    }
}
