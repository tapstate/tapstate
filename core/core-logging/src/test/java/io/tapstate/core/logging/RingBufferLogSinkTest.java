package io.tapstate.core.logging;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RingBufferLogSinkTest {

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
    void pageResumesStrictlyAfterTheReturnedCursor() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        sink.append("pl1", line("a"));
        sink.append("pl1", line("b"));
        sink.append("pl1", line("c"));

        LogPage first = sink.page("pl1", null, 2);
        LogPage second = sink.page("pl1", first.nextCursor(), 2);

        assertThat(first.lines()).extracting(LogLine::message).containsExactly("b", "c");
        assertThat(first.nextCursor()).isNotNull();
        assertThat(second.lines()).isEmpty();
        assertThat(second.nextCursor()).isEqualTo(first.nextCursor());
        assertThat(second.truncated()).isFalse();
    }

    @Test
    void pageMarksAnEvictedCursorAsTruncatedAndContinuesAtTheOldestRetainedLine() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 2);
        sink.append("pl1", line("a"));
        LogCursor beforeEviction = sink.page("pl1", null, 8).nextCursor();
        sink.append("pl1", line("b"));
        sink.append("pl1", line("c"));

        LogPage page = sink.page("pl1", beforeEviction, 8);

        assertThat(page.lines()).extracting(LogLine::message).containsExactly("b", "c");
        assertThat(page.truncated()).isTrue();
    }

    @Test
    void pageMarksACursorFromARecreatedPipelineBufferAsTruncated() {
        RingBufferLogSink sink = new RingBufferLogSink(1, 8);
        sink.append("pl1", line("first"));
        LogCursor cursor = sink.page("pl1", null, 8).nextCursor();

        sink.append("pl2", line("other"));
        sink.append("pl1", line("replacement"));

        LogPage page = sink.page("pl1", cursor, 8);

        assertThat(page.lines()).extracting(LogLine::message).containsExactly("replacement");
        assertThat(page.truncated()).isTrue();
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
