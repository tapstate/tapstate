package io.tapstate.control.core;

import io.tapstate.core.logging.LogCursor;
import io.tapstate.core.logging.LogPage;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.PipelineLogLevel;

import java.util.Objects;

/**
 * The pipeline logs read side. It tails the node-local log sink for one pipeline — the process's own
 * captured log output, keyed by pipeline id. This is the read face that is not store-backed: it reads
 * an in-process sink directly rather than the published observation doc, because logs are node-local
 * and not fanned into a shared store. A read of a pipeline with no captured lines is a benign empty
 * tail, never a coded error.
 */
public final class PipelineLogQueryService {

    private final LogSink logs;

    public PipelineLogQueryService(LogSink logs) {
        this.logs = Objects.requireNonNull(logs, "logs");
    }

    /** The pipeline's most recent log lines, oldest to newest; empty when it has logged nothing here. */
    public PipelineLogs logs(String pipelineId) {
        return logs(pipelineId, null, Integer.MAX_VALUE);
    }

    /** The pipeline's most recent {@code limit} lines, oldest to newest. */
    public PipelineLogs logs(String pipelineId, int limit) {
        return logs(pipelineId, null, limit);
    }

    /**
     * Reads the retained log lines strictly after {@code after}. A missing cursor returns the newest
     * retained tail, while an expired cursor returns the oldest available page and marks it truncated.
     */
    public PipelineLogs logs(String pipelineId, LogCursor after, int limit) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        LogPage page = logs.page(pipelineId, after, limit);
        return new PipelineLogs(pipelineId, page.lines(), page.nextCursor(), page.truncated());
    }

    /** Changes the minimum severity retained for future node-local lines of one pipeline. */
    public PipelineLogLevel level(String pipelineId, PipelineLogLevel level) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(level, "level");
        logs.level(pipelineId, level);
        return logs.level(pipelineId);
    }

    /** Returns the minimum severity retained for future node-local lines of one pipeline. */
    public PipelineLogLevel level(String pipelineId) {
        return logs.level(Objects.requireNonNull(pipelineId, "pipelineId"));
    }
}
