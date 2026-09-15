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

    /**
     * Returns the most recent buffered lines for a pipeline, oldest to newest, as an immutable
     * snapshot. Empty when the pipeline has logged nothing (or is unknown to this node).
     */
    List<LogLine> tail(String pipelineId);

    /**
     * Reads a resumable page after {@code after}. Production sinks with bounded retention override this
     * method with their stable append sequence. The fallback keeps existing lightweight sinks source
     * compatible while providing append-only test doubles with a deterministic cursor.
     */
    default LogPage page(String pipelineId, LogCursor after, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        List<LogLine> lines = tail(pipelineId);
        String generation = "tail";
        boolean truncated = after != null && !generation.equals(after.generation());
        int from;
        if (after == null) {
            from = Math.max(0, lines.size() - limit);
        } else if (truncated) {
            from = 0;
        } else {
            from = (int) Math.min(after.sequence(), lines.size());
        }
        int to = (int) Math.min((long) lines.size(), (long) from + limit);
        LogCursor next = to == 0 ? after : new LogCursor(generation, to);
        return new LogPage(lines.subList(from, to), next, truncated);
    }
}
