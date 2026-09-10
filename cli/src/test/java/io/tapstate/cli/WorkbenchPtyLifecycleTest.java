package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchPtyLifecycleTest {

    private static final Duration SCREEN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(5);
    private static final String ENTER_ALTERNATE_SCREEN = "\u001b[?1049h";
    private static final String LEAVE_ALTERNATE_SCREEN = "\u001b[?1049l";
    private static final String HIDE_CURSOR = "\u001b[?25l";
    private static final String SHOW_CURSOR = "\u001b[?25h";

    @Test
    void sigwinchFrom83By53To88By24RendersTheShellWithoutReplacingTheRunner(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = startBare(home)) {
            long originalJvm = pty.jvmPid();
            assertThat(count(pty.transcript(), ENTER_ALTERNATE_SCREEN)).isEqualTo(1);

            pty.resize(88, 24);
            pty.awaitText("Tapstate", SCREEN_TIMEOUT);

            assertThat(pty.jvmPid()).isEqualTo(originalJvm);
            assertThat(count(pty.transcript(), ENTER_ALTERNATE_SCREEN)).isEqualTo(1);
            pty.send("q");
            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isZero();
            assertTerminalRecovered(pty);
        }
    }

    @Test
    void explicitWorkbenchBackendWinsWhenAnotherProviderIsDiscoverable(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = JvmPtyFixture.start(home, JvmPtyFixture.Mode.BACKEND_SELECTION)) {
            pty.awaitText("__TAPSTATE_BACKEND__io.tapstate.cli.WorkbenchTerminalBackend", SCREEN_TIMEOUT);
            pty.send("q");

            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isZero();
            assertTerminalRecoveredExactlyOnce(pty);
        }
    }

    @Test
    void normalQuitReturnsZeroAndRestoresTheTerminal(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = startBare(home)) {
            pty.send("q");

            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isZero();
            assertTerminalRecovered(pty);
        }
    }

    @Test
    void terminalEofStopsTheWorkbenchAndRestoresTheTerminal(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = startBare(home)) {
            pty.closeInput();

            assertThat(pty.awaitExit(EXIT_TIMEOUT))
                    .as("EOF must stop the workbench instead of leaving its render loop alive; transcript=%s",
                            pty.visibleTranscript())
                    .isTrue();
            assertTerminalRecoveredExactlyOnce(pty);
        }
    }

    @Test
    void injectedEventExceptionLeavesThroughTheSameTerminalCleanup(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = JvmPtyFixture.start(home, JvmPtyFixture.Mode.INJECTED_EXCEPTION)) {
            pty.awaitText("Terminal size too small:", SCREEN_TIMEOUT);
            pty.send("x");
            pty.awaitText("Injected", SCREEN_TIMEOUT);
            pty.send("q");

            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isZero();
            assertTerminalRecoveredExactlyOnce(pty);
        }
    }

    @Test
    void exceptionEscapingTheRunnerBoundaryRestoresTheTerminalExactlyOnce(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = JvmPtyFixture.start(home, JvmPtyFixture.Mode.EXCEPTIONAL_UNWIND)) {
            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isNotZero();
            assertThat(pty.transcript()).contains("Injected runner-boundary failure");
            assertTerminalRecoveredExactlyOnce(pty);
        }
    }

    @Test
    void sigintRestoresTheTerminal(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = startBare(home)) {
            pty.signal("INT");

            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isZero();
            assertTerminalRecoveredExactlyOnce(pty);
        }
    }

    @Test
    void sigtermRestoresTheTerminal(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = startBare(home)) {
            pty.signal("TERM");

            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isNotZero();
            assertTerminalRecoveredExactlyOnce(pty);
        }
    }

    @Test
    void normalQuitEmitsEachTerminalRestorationOnce(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = startBare(home)) {
            pty.send("q");
            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();

            assertThat(count(pty.transcript(), LEAVE_ALTERNATE_SCREEN))
                    .as("alternate-screen restoration")
                    .isEqualTo(1);
            assertThat(count(pty.transcript(), SHOW_CURSOR))
                    .as("cursor restoration")
                    .isEqualTo(1);
            assertThat(pty.finalTerminalState())
                    .as("cooked-mode restoration")
                    .isEqualTo(pty.initialTerminalState());
        }
    }

    @Test
    void setupFailureRestoresAndClosesTheTerminalExactlyOnce(@TempDir Path home) throws Exception {
        try (JvmPtyFixture pty = JvmPtyFixture.start(home, JvmPtyFixture.Mode.SETUP_FAILURE)) {
            assertThat(pty.awaitExit(EXIT_TIMEOUT)).as(pty::visibleTranscript).isTrue();
            assertThat(pty.childExitStatus()).isZero();
            assertTerminalRecovered(pty);
            assertThat(count(pty.transcript(), LEAVE_ALTERNATE_SCREEN)).isEqualTo(1);
            assertThat(count(pty.transcript(), SHOW_CURSOR)).isEqualTo(1);
        }
    }

    private static JvmPtyFixture startBare(Path home) throws Exception {
        JvmPtyFixture pty = JvmPtyFixture.start(home, JvmPtyFixture.Mode.BARE);
        pty.awaitText("Terminal size too small:", SCREEN_TIMEOUT);
        return pty;
    }

    private static void assertTerminalRecovered(JvmPtyFixture pty) {
        String transcript = pty.transcript();
        assertThat(pty.finalTerminalState()).as(pty::visibleTranscript).isEqualTo(pty.initialTerminalState());
        assertThat(transcript).contains(ENTER_ALTERNATE_SCREEN, LEAVE_ALTERNATE_SCREEN, HIDE_CURSOR, SHOW_CURSOR);
        assertThat(transcript.lastIndexOf(LEAVE_ALTERNATE_SCREEN))
                .isGreaterThan(transcript.lastIndexOf(ENTER_ALTERNATE_SCREEN));
        assertThat(transcript.lastIndexOf(SHOW_CURSOR)).isGreaterThan(transcript.lastIndexOf(HIDE_CURSOR));
    }

    private static void assertTerminalRecoveredExactlyOnce(JvmPtyFixture pty) {
        assertTerminalRecovered(pty);
        assertThat(count(pty.transcript(), LEAVE_ALTERNATE_SCREEN))
                .as("alternate-screen restoration")
                .isEqualTo(1);
        assertThat(count(pty.transcript(), SHOW_CURSOR))
                .as("cursor restoration")
                .isEqualTo(1);
    }

    private static int count(String value, String needle) {
        int matches = 0;
        int index = 0;
        while ((index = value.indexOf(needle, index)) >= 0) {
            matches++;
            index += needle.length();
        }
        return matches;
    }
}
