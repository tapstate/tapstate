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
 * pipeline (oldest evicted first) and tracks at most a fixed number of pipelines (the pipeline that
 * has gone longest without a new line is dropped whole). Both bounds cap memory so a long-running or
 * noisy process cannot grow the buffer without limit. All operations are thread-safe: the runtime
 * appends from its own threads while a control read face tails concurrently.
 */
public final class RingBufferLogSink implements LogSink {

    private final int maxLinesPerPipeline;
    private final Map<String, PipelineBuffer> byPipeline;

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
            protected boolean removeEldestEntry(Map.Entry<String, PipelineBuffer> eldest) {
                return size() > maxPipelines;
            }
        };
    }

    @Override
    public synchronized void append(String pipelineId, LogLine line) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(line, "line");
        // Remove then re-insert so this pipeline becomes the most-recently-appended entry (insertion
        // order is the recency order the cardinality bound evicts against).
        PipelineBuffer buffer = byPipeline.remove(pipelineId);
        if (buffer == null) {
            buffer = new PipelineBuffer();
        }
        buffer.append(line);
        while (buffer.lines.size() > maxLinesPerPipeline) {
            buffer.lines.removeFirst();
        }
        byPipeline.put(pipelineId, buffer);
    }

    @Override
    public synchronized List<LogLine> tail(String pipelineId) {
        PipelineBuffer buffer = byPipeline.get(pipelineId);
        if (buffer == null) {
            return List.of();
        }
        return buffer.lines.stream().map(SequencedLine::line).toList();
    }

    @Override
    public synchronized LogPage page(String pipelineId, LogCursor after, int limit) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        PipelineBuffer buffer = byPipeline.get(pipelineId);
        if (buffer == null || buffer.lines.isEmpty()) {
            return new LogPage(List.of(), null, after != null);
        }

        List<SequencedLine> retained = List.copyOf(buffer.lines);
        boolean truncated = after != null && !buffer.generation.equals(after.generation());
        int from;
        if (after == null) {
            from = Math.max(0, retained.size() - limit);
        } else if (truncated) {
            from = 0;
        } else {
            long oldest = retained.getFirst().sequence();
            if (after.sequence() < oldest - 1) {
                truncated = true;
                from = 0;
            } else {
                from = firstAfter(retained, after.sequence());
            }
        }
        int to = (int) Math.min((long) retained.size(), (long) from + limit);
        List<LogLine> lines = retained.subList(from, to).stream().map(SequencedLine::line).toList();
        LogCursor next = to == 0 ? after : new LogCursor(buffer.generation, retained.get(to - 1).sequence());
        return new LogPage(lines, next, truncated);
    }

    private static int firstAfter(List<SequencedLine> lines, long sequence) {
        for (int index = 0; index < lines.size(); index++) {
            if (lines.get(index).sequence() > sequence) {
                return index;
            }
        }
        return lines.size();
    }

    private static final class PipelineBuffer {
        private final String generation = UUID.randomUUID().toString();
        private final Deque<SequencedLine> lines = new ArrayDeque<>();
        private long nextSequence = 1;

        void append(LogLine line) {
            lines.addLast(new SequencedLine(nextSequence++, line));
        }
    }

    private record SequencedLine(long sequence, LogLine line) {
    }
}
