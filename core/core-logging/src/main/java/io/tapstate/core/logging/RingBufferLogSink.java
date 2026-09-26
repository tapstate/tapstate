package io.tapstate.core.logging;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A bounded, in-memory {@link LogSink}. It keeps at most a fixed number of the most recent lines per
 * pipeline (oldest evicted first) and tracks at most a fixed number of pipelines (the pipeline that
 * has gone longest without a new line is dropped whole). Both bounds cap memory so a long-running or
 * noisy process cannot grow the buffer without limit. All operations are thread-safe: the runtime
 * appends from its own threads while a control read face tails concurrently.
 */
public final class RingBufferLogSink implements LogSink {

    private record StoredLine(Scope scope, LogLine line) {
    }

    private final int maxLinesPerPipeline;
    private final Map<String, Deque<StoredLine>> byPipeline;

    /**
     * @param maxPipelines        the most pipelines to retain lines for; the least-recently-appended
     *                            pipeline is evicted when exceeded
     * @param maxLinesPerPipeline the most recent lines to retain per pipeline; the oldest is evicted
     *                            when exceeded
     */
    public RingBufferLogSink(int maxPipelines, int maxLinesPerPipeline) {
        if (maxPipelines < 1 || maxLinesPerPipeline < 1) {
            throw new IllegalArgumentException("bounds must be positive");
        }
        this.maxLinesPerPipeline = maxLinesPerPipeline;
        this.byPipeline = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Deque<StoredLine>> eldest) {
                return size() > maxPipelines;
            }
        };
    }

    @Override
    public synchronized void append(String pipelineId, LogLine line) {
        append(pipelineId, null, line);
    }

    @Override
    public synchronized void append(String pipelineId, Scope scope, LogLine line) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(line, "line");
        // Remove then re-insert so this pipeline becomes the most-recently-appended entry (insertion
        // order is the recency order the cardinality bound evicts against).
        Deque<StoredLine> lines = byPipeline.remove(pipelineId);
        if (lines == null) {
            lines = new ArrayDeque<>();
        }
        lines.addLast(new StoredLine(scope, line));
        while (lines.size() > maxLinesPerPipeline) {
            lines.removeFirst();
        }
        byPipeline.put(pipelineId, lines);
    }

    @Override
    public synchronized List<LogLine> tail(String pipelineId) {
        return select(pipelineId, scope -> scope == null);
    }

    @Override
    public synchronized List<LogLine> tail(String pipelineId, Scope scope) {
        Objects.requireNonNull(scope, "scope");
        return select(pipelineId, scope::equals);
    }

    @Override
    public synchronized List<LogLine> tailIncarnation(String pipelineId, String incarnationId) {
        Objects.requireNonNull(incarnationId, "incarnationId");
        return select(pipelineId, scope -> scope != null
                && incarnationId.equals(scope.pipelineIncarnationId()));
    }

    private List<LogLine> select(String pipelineId, java.util.function.Predicate<Scope> matches) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Deque<StoredLine> lines = byPipeline.get(pipelineId);
        return lines == null ? List.of() : lines.stream().filter(stored -> matches.test(stored.scope()))
                .map(StoredLine::line).toList();
    }

    @Override
    public synchronized void clearIncarnation(String pipelineId, String incarnationId) {
        Objects.requireNonNull(incarnationId, "incarnationId");
        clearMatching(pipelineId, scope -> scope != null
                && incarnationId.equals(scope.pipelineIncarnationId()));
    }

    @Override
    public synchronized void clearLegacy(String pipelineId) {
        clearMatching(pipelineId, scope -> scope == null);
    }

    private void clearMatching(String pipelineId, java.util.function.Predicate<Scope> matches) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Deque<StoredLine> lines = byPipeline.get(pipelineId);
        if (lines != null) {
            lines.removeIf(stored -> matches.test(stored.scope()));
            if (lines.isEmpty()) {
                byPipeline.remove(pipelineId);
            }
        }
    }
}
