package io.tapstate.core.logging;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A bounded, in-memory {@link LogSink}. It keeps at most a fixed number of the most recent lines per
 * pipeline (older generations evicted first, then oldest within a generation) and tracks at most a fixed number of pipelines (the pipeline that
 * has gone longest without a new line is dropped whole). Both bounds cap memory so a long-running or
 * noisy process cannot grow the buffer without limit. All operations are thread-safe: the runtime
 * appends from its own threads while a control read face tails concurrently.
 */
public final class RingBufferLogSink implements LogSink {

    private static final long UNSCOPED = 0;

    private record Entry(long generation, LogLine line) {}

    private static final class Buffer {
        final Deque<Entry> lines = new ArrayDeque<>();
        long firstGeneration;
    }

    private final int maxLinesPerPipeline;
    private final Map<String, Buffer> byPipeline;

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
            protected boolean removeEldestEntry(Map.Entry<String, Buffer> eldest) {
                return size() > maxPipelines;
            }
        };
    }

    @Override
    public synchronized void append(String pipelineId, LogLine line) {
        appendLine(pipelineId, UNSCOPED, line);
    }

    @Override
    public synchronized void append(String pipelineId, long generation, LogLine line) {
        if (generation < 1) {
            throw new IllegalArgumentException("a scoped log generation must be positive");
        }
        appendLine(pipelineId, generation, line);
    }

    private void appendLine(String pipelineId, long generation, LogLine line) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(line, "line");
        Buffer buffer = byPipeline.get(pipelineId);
        if (buffer != null && generation < buffer.firstGeneration) {
            return;
        }
        if (buffer == null) {
            buffer = new Buffer();
        }
        var incoming = new Entry(generation, line);
        buffer.lines.addLast(incoming);
        if (buffer.lines.size() > maxLinesPerPipeline) {
            // A predecessor must not evict a newer line even before the first verified read.
            var evicted = buffer.lines.getFirst();
            for (var entry : buffer.lines) {
                if (entry.generation() < evicted.generation()) {
                    evicted = entry;
                }
            }
            buffer.lines.removeFirstOccurrence(evicted);
            if (evicted == incoming) {
                return;
            }
        }
        // Reinsert only accepted writes so stale producers do not extend a pipeline's log retention.
        byPipeline.remove(pipelineId);
        byPipeline.put(pipelineId, buffer);
    }

    @Override
    public synchronized List<LogLine> tail(String pipelineId) {
        Buffer buffer = byPipeline.get(pipelineId);
        return buffer == null ? List.of() : buffer.lines.stream().map(Entry::line).toList();
    }

    @Override
    public synchronized List<LogLine> tail(String pipelineId, long firstGeneration, long lastGeneration) {
        if (firstGeneration < 1 || lastGeneration < firstGeneration) {
            throw new IllegalArgumentException("invalid log generation window");
        }
        Buffer buffer = byPipeline.get(pipelineId);
        if (buffer == null || firstGeneration < buffer.firstGeneration) {
            return List.of();
        }
        if (firstGeneration > buffer.firstGeneration) {
            // Learning a new lifetime also keeps late predecessors from filling the bounded buffer.
            buffer.lines.removeIf(entry -> entry.generation() < firstGeneration);
            buffer.firstGeneration = firstGeneration;
        }
        return buffer.lines.stream()
                .filter(entry -> entry.generation() <= lastGeneration)
                .map(Entry::line)
                .toList();
    }
}
