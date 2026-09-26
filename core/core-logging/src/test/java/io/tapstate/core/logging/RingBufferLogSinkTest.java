package io.tapstate.core.logging;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RingBufferLogSinkTest {

    @Test
    void recreatedIdAndDelayedOldClearCannotExposeOrRemoveNewLines() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        LogSink.Scope oldRun = new LogSink.Scope("old-resource", 4);
        LogSink.Scope newRun = new LogSink.Scope("new-resource", 5);
        sink.append("orders", oldRun, line("old line"));
        sink.append("orders", newRun, line("new line"));

        assertThat(sink.tail("orders", newRun)).extracting(LogLine::message)
                .containsExactly("new line");
        sink.clearIncarnation("orders", "old-resource");
        assertThat(sink.tail("orders", newRun)).extracting(LogLine::message)
                .containsExactly("new line");
        assertThat(sink.tailIncarnation("orders", "old-resource")).isEmpty();
    }

    @Test
    void scopedExecutionsShareTheExistingLineAndPipelineBounds() {
        RingBufferLogSink sink = new RingBufferLogSink(1, 2);
        LogSink.Scope first = new LogSink.Scope("resource-a", 1);
        LogSink.Scope second = new LogSink.Scope("resource-a", 2);
        sink.append("orders", first, line("oldest"));
        sink.append("orders", second, line("newer"));
        sink.append("orders", second, line("newest"));
        assertThat(sink.tail("orders", first)).isEmpty();
        assertThat(sink.tail("orders", second)).extracting(LogLine::message)
                .containsExactly("newer", "newest");

        sink.append("invoices", new LogSink.Scope("resource-b", 1), line("another pipeline"));
        assertThat(sink.tailIncarnation("orders", "resource-a")).isEmpty();
    }

    private static LogLine line(String message) {
        return new LogLine(0L, "INFO", message);
    }

    @Test
    void appendedLineIsReturnedInTheTail() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        sink.append("pl1", line("first"));

        assertThat(sink.tail("pl1")).extracting(LogLine::message).containsExactly("first");
    }

    @Test
    void tailIsScopedToTheRequestedPipeline() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        sink.append("pl1", line("a"));
        sink.append("pl2", line("b"));

        assertThat(sink.tail("pl1")).extracting(LogLine::message).containsExactly("a");
        assertThat(sink.tail("pl2")).extracting(LogLine::message).containsExactly("b");
    }

    @Test
    void anUnknownPipelineTailIsEmpty() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        // A tail of a pipeline that has logged nothing (or does not exist) is a benign empty result,
        // never an error: the absence of log lines is normal.
        assertThat(sink.tail("never-logged")).isEmpty();
    }

    @Test
    void tailKeepsOnlyTheMostRecentLinesPerPipelineInOrder() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 3);

        for (int i = 1; i <= 5; i++) {
            sink.append("pl1", line(Integer.toString(i)));
        }

        // Oldest lines are evicted once the per-pipeline bound is reached; the tail is the most recent
        // lines, oldest to newest.
        assertThat(sink.tail("pl1")).extracting(LogLine::message).containsExactly("3", "4", "5");
    }

    @Test
    void tailIsAnImmutableSnapshotUnaffectedByLaterAppends() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        sink.append("pl1", line("a"));

        List<LogLine> snapshot = sink.tail("pl1");
        sink.append("pl1", line("b"));

        assertThat(snapshot).extracting(LogLine::message).containsExactly("a");
        assertThatThrownBy(() -> snapshot.add(line("x"))).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void pipelineCardinalityIsBoundedEvictingTheLeastRecentlyAppended() {
        RingBufferLogSink sink = new RingBufferLogSink(2, 8);

        sink.append("pl1", line("a"));
        sink.append("pl2", line("b"));
        sink.append("pl3", line("c"));

        // Only the two most-recently-appended pipelines are retained; the least recent is dropped whole.
        assertThat(sink.tail("pl1")).isEmpty();
        assertThat(sink.tail("pl2")).extracting(LogLine::message).containsExactly("b");
        assertThat(sink.tail("pl3")).extracting(LogLine::message).containsExactly("c");
    }

    @Test
    void reAppendingToAPipelineKeepsItFromEviction() {
        RingBufferLogSink sink = new RingBufferLogSink(2, 8);

        sink.append("pl1", line("a"));
        sink.append("pl2", line("b"));
        sink.append("pl1", line("a2")); // pl1 is now most-recently-appended
        sink.append("pl3", line("c")); // evicts pl2 (least recently appended), not pl1

        assertThat(sink.tail("pl1")).extracting(LogLine::message).containsExactly("a", "a2");
        assertThat(sink.tail("pl2")).isEmpty();
        assertThat(sink.tail("pl3")).extracting(LogLine::message).containsExactly("c");
    }

    @Test
    void rejectsNonPositiveBounds() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RingBufferLogSink(0, 8));
        assertThatIllegalArgumentException().isThrownBy(() -> new RingBufferLogSink(8, 0));
    }
}
