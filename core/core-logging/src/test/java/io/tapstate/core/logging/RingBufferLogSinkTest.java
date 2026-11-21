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
    void aVerifiedLifetimeKeepsItsRebuiltGenerationsButNeverShowsItsPredecessor() {
        RingBufferLogSink sink = new RingBufferLogSink(2, 8);
        sink.append("flow", line("unattributed lifetime"));
        sink.append("flow", 2, line("retired pipeline"));
        sink.append("flow", 3, line("created"));
        sink.append("other", 2, line("different pipeline"));
        sink.append("flow", 4, line("rebuilt"));

        assertThat(sink.tail("flow", 3, 3)).extracting(LogLine::message).containsExactly("created");
        assertThat(sink.tail("flow", 3, 4)).extracting(LogLine::message).containsExactly("created", "rebuilt");
        assertThat(sink.tail("other", 2, 2)).extracting(LogLine::message).containsExactly("different pipeline");
        assertThat(sink.tail("flow", 1, 2)).isEmpty();
    }

    @Test
    void aLatePredecessorCannotFillTheSuccessorsBoundedBuffer() {
        RingBufferLogSink sink = new RingBufferLogSink(1, 2);
        sink.append("flow", 1, line("retired"));
        sink.append("flow", 3, line("replacement initialized"));
        assertThat(sink.tail("flow", 3, 3)).extracting(LogLine::message).containsExactly("replacement initialized");
        sink.append("flow", 3, line("replacement running"));

        for (int i = 0; i < 3; i++) {
            sink.append("flow", 1, line("late predecessor"));
        }

        assertThat(sink.tail("flow", 3, 3)).extracting(LogLine::message)
                .containsExactly("replacement initialized", "replacement running");
    }

    @Test
    void lateOlderLinesCannotCrowdOutNewerLinesBeforeAnyScopedRead() {
        RingBufferLogSink sink = new RingBufferLogSink(1, 2);
        sink.append("flow", 3, line("replacement initialized"));
        sink.append("flow", 1, line("late first"));
        sink.append("flow", 1, line("late second"));
        sink.append("flow", 3, line("replacement running"));

        assertThat(sink.tail("flow", 3, 3)).extracting(LogLine::message)
                .containsExactly("replacement initialized", "replacement running");
    }

    @Test
    void rejectsNonPositiveBounds() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RingBufferLogSink(0, 8));
        assertThatIllegalArgumentException().isThrownBy(() -> new RingBufferLogSink(8, 0));
    }
    @Test
    void legacyLinesRemainReadableAcrossScopedLifetimeChanges() {
        var sink = new RingBufferLogSink(2, 4);
        var before = new LogLine(1L, "INFO", "unscoped before");
        var scoped = new LogLine(2L, "INFO", "scoped");
        var after = new LogLine(3L, "INFO", "unscoped after");
        sink.append("orders", before);
        sink.append("orders", 2L, scoped);
        assertThat(sink.tail("orders", 2L, 2L)).containsExactly(scoped);
        sink.append("orders", after);
        assertThat(sink.tail("orders")).containsExactly(before, scoped, after);
        assertThat(sink.tail("orders", 2L, 2L)).containsExactly(scoped);
    }
}
