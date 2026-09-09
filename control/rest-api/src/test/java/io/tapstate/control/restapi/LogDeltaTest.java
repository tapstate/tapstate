package io.tapstate.control.restapi;

import io.tapstate.core.logging.LogLine;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pure log-tail delta: given the lines a follower has already been sent and the current node-local
 * tail, which lines are new. The common case is an append (the ring grew); eviction shifts the window,
 * so the anchor (the last already-sent line) is located by its last occurrence and only the lines after
 * it are new. When the anchor is gone entirely — a burst evicted it — the whole current tail is re-sent
 * (the accepted worst case: re-attach and continue tailing), which is honest for a bounded node-local
 * sink rather than silently dropping a gap.
 */
class LogDeltaTest {

    private static LogLine line(long ts, String message) {
        return new LogLine(ts, "INFO", message);
    }

    @Test
    void everyLineIsNewWhenNothingHasBeenSentYet() {
        List<LogLine> current = List.of(line(1, "a"), line(2, "b"));
        assertThat(LogDelta.newLines(List.of(), current)).isEqualTo(current);
    }

    @Test
    void onlyTheAppendedSuffixIsNewOnGrowth() {
        List<LogLine> previous = List.of(line(1, "a"), line(2, "b"));
        List<LogLine> current = List.of(line(1, "a"), line(2, "b"), line(3, "c"), line(4, "d"));
        assertThat(LogDelta.newLines(previous, current)).containsExactly(line(3, "c"), line(4, "d"));
    }

    @Test
    void nothingIsNewWhenTheTailIsUnchanged() {
        List<LogLine> tail = List.of(line(1, "a"), line(2, "b"));
        assertThat(LogDelta.newLines(tail, tail)).isEmpty();
    }

    @Test
    void findsTheAnchorAfterOlderLinesWereEvicted() {
        List<LogLine> previous = List.of(line(1, "a"), line(2, "b"), line(3, "c"));
        // a and b aged out; c is still the newest already-sent line, d is new
        List<LogLine> current = List.of(line(3, "c"), line(4, "d"));
        assertThat(LogDelta.newLines(previous, current)).containsExactly(line(4, "d"));
    }

    @Test
    void reSendsTheWholeTailWhenTheAnchorWasEvicted() {
        List<LogLine> previous = List.of(line(1, "a"), line(2, "b"));
        // a burst pushed a and b out before the follower polled: the anchor is gone, re-tail
        List<LogLine> current = List.of(line(5, "e"), line(6, "f"));
        assertThat(LogDelta.newLines(previous, current)).containsExactly(line(5, "e"), line(6, "f"));
    }

    @Test
    void nothingIsNewWhenTheCurrentTailIsEmpty() {
        assertThat(LogDelta.newLines(List.of(line(1, "a")), List.of())).isEmpty();
    }

    @Test
    void doesNotDropNewLinesWhenTheLastSentLineRecursLater() {
        // 'a' was already sent; then 'b' and an identical 'a' were appended -- both are new, neither dropped.
        // Anchoring on the *last* occurrence of 'a' would skip them; the overlap must anchor on the prefix.
        List<LogLine> previous = List.of(line(1, "a"));
        List<LogLine> current = List.of(line(1, "a"), line(2, "b"), line(1, "a"));
        assertThat(LogDelta.newLines(previous, current)).containsExactly(line(2, "b"), line(1, "a"));
    }

    @Test
    void anchorsOnTheOverlapNotACoincidentalLaterRepeatAfterEviction() {
        // 'x' aged out; the sent tail ended at 'a', which is the window's leading line; b, a, c are all new.
        List<LogLine> previous = List.of(line(0, "x"), line(1, "a"));
        List<LogLine> current = List.of(line(1, "a"), line(2, "b"), line(1, "a"), line(3, "c"));
        assertThat(LogDelta.newLines(previous, current))
                .containsExactly(line(2, "b"), line(1, "a"), line(3, "c"));
    }
}
