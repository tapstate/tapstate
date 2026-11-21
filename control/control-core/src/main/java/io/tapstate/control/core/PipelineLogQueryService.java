package io.tapstate.control.core;

import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.LogLine;
import io.tapstate.spi.store.PipelineStateMappings.LogWindow;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The pipeline logs read side. It tails the node-local log sink for one pipeline — the process's own
 * captured log output. Lines stay node-local; only the scoped read resolves the current lifetime
 * through configuration. A pipeline with no visible lines has a benign empty tail.
 */
public final class PipelineLogQueryService {

    private final Function<String, List<LogLine>> tail;

    public PipelineLogQueryService(LogSink logs) {
        this(Objects.requireNonNull(logs, "logs")::tail);
    }

    private PipelineLogQueryService(Function<String, List<LogLine>> tail) {
        this.tail = tail;
    }

    public static PipelineLogQueryService scoped(LogSink logs, Function<String, Optional<LogWindow>> windows) {
        Objects.requireNonNull(logs, "logs");
        Objects.requireNonNull(windows, "windows");
        return new PipelineLogQueryService(id -> {
            Optional<LogWindow> window = windows.apply(id);
            for (int attempt = 0; attempt < 2 && window.isPresent(); attempt++) {
                var original = window.orElseThrow();
                var lines = logs.tail(id, original.firstGeneration(), original.lastGeneration());
                window = windows.apply(id);
                if (window.filter(original::equals).isPresent()) return lines;
            }
            return List.of();
        });
    }

    /** The pipeline's most recent log lines, oldest to newest; empty when it has logged nothing here. */
    public PipelineLogs logs(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return new PipelineLogs(pipelineId, tail.apply(pipelineId));
    }

    /** The pipeline's most recent {@code limit} lines, oldest to newest. */
    public PipelineLogs logs(String pipelineId, int limit) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        List<LogLine> lines = tail.apply(pipelineId);
        int from = Math.max(0, lines.size() - limit);
        return new PipelineLogs(pipelineId, lines.subList(from, lines.size()));
    }
}
