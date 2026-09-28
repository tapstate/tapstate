package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a follow has already shown, and therefore what an attach after it owes the reader. Every case
 * here is about one boundary: a re-attach opens with a whole window, and only the part of that window
 * nobody has seen is new.
 */
class PrintedLogTailTest {

    private static final RemoteLogLine STARTED = line(1, "starting");
    private static final RemoteLogLine RUNNING = line(2, "running");
    private static final RemoteLogLine SLOW = line(3, "slow tick");

    private final PrintedLogTail tail = new PrintedLogTail();

    @Test
    void theFirstWindowIsAllNew() {
        tail.attaching();

        assertThat(tail.notYetPrinted(List.of(STARTED, RUNNING))).containsExactly(STARTED, RUNNING);
    }

    @Test
    void aWindowResentWholeAfterAReattachShowsNothing() {
        tail.attaching();
        tail.notYetPrinted(List.of(STARTED, RUNNING));

        tail.attaching();

        assertThat(tail.notYetPrinted(List.of(STARTED, RUNNING)))
                .as("the same window again is the same lines, not a pipeline that logged them twice")
                .isEmpty();
    }

    @Test
    void aWindowThatGrewWhileTheStreamWasDownShowsOnlyWhatGrew() {
        tail.attaching();
        tail.notYetPrinted(List.of(STARTED, RUNNING));

        tail.attaching();

        assertThat(tail.notYetPrinted(List.of(STARTED, RUNNING, SLOW))).containsExactly(SLOW);
    }

    @Test
    void aWindowWithNothingInCommonIsAllNew() {
        tail.attaching();
        tail.notYetPrinted(List.of(STARTED, RUNNING));

        tail.attaching();

        assertThat(tail.notYetPrinted(List.of(SLOW)))
                .as("another member's tail shares no history with this one, and a burst can evict a "
                        + "whole window: showing all of it repeats at worst, and loses nothing")
                .containsExactly(SLOW);
    }

    @Test
    void aLineThatRepeatsWhileTheStreamIsUpIsStillShown() {
        tail.attaching();
        tail.notYetPrinted(List.of(STARTED, RUNNING));

        assertThat(tail.notYetPrinted(List.of(RUNNING)))
                .as("frames after the opening one are deltas the server computed against what it had "
                        + "already sent; measuring them again would swallow a line that genuinely recurs, "
                        + "and a recurring line is what somebody following logs is most likely counting")
                .containsExactly(RUNNING);
    }

    @Test
    void anOverlapIsMeasuredAgainstTheWholeWindowNotTheLastLineAlone() {
        tail.attaching();
        tail.notYetPrinted(List.of(STARTED, RUNNING, RUNNING));

        tail.attaching();

        assertThat(tail.notYetPrinted(List.of(RUNNING, RUNNING, SLOW)))
                .as("anchoring on the last line's last occurrence would cut the window in the wrong place")
                .containsExactly(SLOW);
    }

    private static RemoteLogLine line(long at, String message) {
        return new RemoteLogLine(at, "INFO", message);
    }
}
