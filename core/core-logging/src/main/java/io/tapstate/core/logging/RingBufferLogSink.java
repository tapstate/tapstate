package io.tapstate.core.logging;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A bounded, in-memory {@link LogSink}. It keeps at most a fixed number of the most recent lines per
 * pipeline (older generations evicted first, then oldest within a generation) and tracks at most a fixed number of pipelines (the pipeline that
 * has gone longest without a new line is dropped whole). Both bounds cap memory so a long-running or
 * noisy process cannot grow the buffer without limit. All operations are thread-safe: the runtime
 * appends from its own threads while a control read face tails concurrently.
 */
public final class RingBufferLogSink implements LogSink {

    private static final long UNSCOPED = 0;

    private record Entry(long generation, long sequence, LogLine line) {}

    private static final class Buffer {
        final String cursorGeneration = UUID.randomUUID().toString();
        long nextSequence = 1;
        final Deque<Entry> lines = new ArrayDeque<>();
        long firstGeneration;
        boolean mixed;
    }

    private final int maxLinesPerPipeline;
    private final Map<String, Buffer> byPipeline;
    private final Map<String, PipelineLogLevel> levels = new LinkedHashMap<>();

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
        if (!level(pipelineId).accepts(line.level())) return;
        Buffer buffer = byPipeline.get(pipelineId);
        if (buffer != null && generation != UNSCOPED && generation < buffer.firstGeneration) {
            return;
        }
        if (buffer == null) {
            buffer = new Buffer();
        }
        var incoming = new Entry(generation, buffer.nextSequence++, line);
        if (!buffer.lines.isEmpty() && buffer.lines.getFirst().generation() != generation) buffer.mixed = true;
        buffer.lines.addLast(incoming);
        if (buffer.lines.size() > maxLinesPerPipeline && evictOldest(buffer) == incoming) return;
        // Reinsert only accepted writes so stale producers do not extend a pipeline's log retention.
        byPipeline.remove(pipelineId);
        byPipeline.put(pipelineId, buffer);
    }

    private static Entry evictOldest(Buffer buffer) {
        if (!buffer.mixed) return buffer.lines.removeFirst();
        // A predecessor must not evict a newer line even before the first verified read.
        var oldest = buffer.lines.getFirst();
        for (var entry : buffer.lines) {
            if (entry.generation() < oldest.generation()) oldest = entry;
        }
        buffer.lines.removeFirstOccurrence(oldest);
        refreshMixed(buffer);
        return oldest;
    }

    private static void refreshMixed(Buffer buffer) {
        var first = buffer.lines.peekFirst();
        buffer.mixed = first != null && buffer.lines.stream().anyMatch(line -> line.generation() != first.generation());
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
            buffer.lines.removeIf(entry -> entry.generation() != UNSCOPED && entry.generation() < firstGeneration);
            refreshMixed(buffer);
            buffer.firstGeneration = firstGeneration;
        }
        return buffer.lines.stream()
                .filter(entry -> entry.generation() >= firstGeneration && entry.generation() <= lastGeneration)
                .map(Entry::line)
                .toList();
    }
    @Override
    public synchronized void level(String pipelineId, PipelineLogLevel level) {
        levels.put(Objects.requireNonNull(pipelineId, "pipelineId"), Objects.requireNonNull(level, "level"));
    }

    @Override
    public synchronized PipelineLogLevel level(String pipelineId) {
        return levels.getOrDefault(Objects.requireNonNull(pipelineId, "pipelineId"), PipelineLogLevel.INFO);
    }

    @Override
    public synchronized LogPage page(String pipelineId, LogCursor after, int limit) {
        return page(pipelineId, after, limit, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Override
    public synchronized LogPage page(String pipelineId, LogCursor after, int limit,
            long firstGeneration, long lastGeneration) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (limit < 1) throw new IllegalArgumentException("limit must be positive");
        if (firstGeneration > lastGeneration) throw new IllegalArgumentException("invalid log generation window");
        if (firstGeneration > 0) tail(pipelineId, firstGeneration, lastGeneration);
        Buffer buffer = byPipeline.get(pipelineId);
        if (buffer == null || firstGeneration < buffer.firstGeneration && firstGeneration > 0) {
            return new LogPage(List.of(), null, after != null);
        }
        var retained = buffer.lines.stream()
                .filter(entry -> entry.generation() >= firstGeneration && entry.generation() <= lastGeneration)
                .toList();
        String cursorGeneration = buffer.cursorGeneration + ":" + firstGeneration;
        boolean truncated = after != null && !cursorGeneration.equals(after.generation());
        int from = 0;
        if (after == null) {
            from = Math.max(0, retained.size() - limit);
        } else if (!truncated && !retained.isEmpty()) {
            if (after.sequence() < retained.getFirst().sequence()) {
                truncated = true;
            } else {
                while (from < retained.size() && retained.get(from).sequence() <= after.sequence()) from++;
            }
        }
        int to = (int) Math.min((long) retained.size(), (long) from + limit);
        var lines = retained.subList(from, to).stream().map(Entry::line).toList();
        LogCursor next = to == 0 ? (truncated ? null : after)
                : new LogCursor(cursorGeneration, retained.get(to - 1).sequence());
        return new LogPage(lines, next, truncated);
    }

}
