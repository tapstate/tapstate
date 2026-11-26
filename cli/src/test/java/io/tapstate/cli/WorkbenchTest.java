package io.tapstate.cli;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkbenchTest {

    @Test
    void terminalOpenFailureRetainsItsStartupDiagnostic() {
        var stderr = new StringWriter();

        int result = Workbench.run(new PrintWriter(stderr), () -> {
            throw new IOException("terminal unavailable");
        }, runner -> { throw new AssertionError("session must not run"); });

        assertThat(result).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(stderr.toString()).contains(CliError.WORKBENCH_UNAVAILABLE.code());
    }

    @Test
    void checkedSessionFailureIsDistinguishedFromStartupFailure() {
        var stderr = new StringWriter();

        int result = Workbench.run(new PrintWriter(stderr), WorkbenchTest::interactiveTerminal,
                runner -> { throw new IOException("terminal I/O stopped"); });

        assertThat(result).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(stderr.toString()).contains(CliError.WORKBENCH_SESSION_FAILED.code())
                .doesNotContain(CliError.WORKBENCH_UNAVAILABLE.code(), "terminal I/O stopped");
    }

    @Test
    void programmerFailureEscapesAfterTheTerminalIsRestored() {
        var stderr = new StringWriter();
        var failure = new IllegalStateException("broken workbench invariant");
        var terminalClosed = new AtomicBoolean();

        assertThatThrownBy(() -> Workbench.run(new PrintWriter(stderr), () -> new DumbTerminal(
                "test", "xterm", new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream(),
                StandardCharsets.UTF_8) {
            @Override
            protected void doClose() throws IOException {
                terminalClosed.set(true);
                super.doClose();
            }
        }, runner -> { throw failure; })).isSameAs(failure);

        assertThat(terminalClosed).isTrue();
        assertThat(stderr.toString()).isEmpty();
    }

    private static DumbTerminal interactiveTerminal() throws IOException {
        return new DumbTerminal("test", "xterm", new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream(), StandardCharsets.UTF_8);
    }


    @Test
    void runnerConfigurationDisablesPeriodicTicks() throws Exception {
        WorkbenchTerminalBackend backend = new WorkbenchTerminalBackend(new DumbTerminal(
                new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream()));
        try {
            TuiConfig config = Workbench.runnerConfig(backend);

            assertThat(config.ticksEnabled()).isFalse();
        } finally {
            backend.close();
        }
    }

    @Test
    void frameBelowTheMinimumShowsTheLiveTerminalDimensions() {
        Buffer buffer = Buffer.empty(new Rect(0, 0, 83, 53));

        Workbench.render(Frame.forTesting(buffer));

        assertThat(textOf(buffer))
                .contains("Terminal size too small:")
                .contains("Width = 83  Height = 53")
                .contains("Needed for current config:")
                .contains("Width = 88  Height = 24")
                .doesNotContain("TapState");
    }

    @Test
    void frameBelowTheMinimumHeightShowsTheLiveTerminalDimensions() {
        Buffer buffer = Buffer.empty(new Rect(0, 0, 88, 23));

        Workbench.render(Frame.forTesting(buffer));

        assertThat(textOf(buffer))
                .contains("Width = 88  Height = 23")
                .contains("Width = 88  Height = 24")
                .doesNotContain("TapState");
    }

    @Test
    void frameAtTheMinimumLeavesTheTooSmallState() {
        Buffer tooSmall = Buffer.empty(new Rect(0, 0, 83, 53));
        Buffer recovered = Buffer.empty(new Rect(0, 0, 88, 24));

        Workbench.render(Frame.forTesting(tooSmall));
        Workbench.render(Frame.forTesting(recovered));

        assertThat(textOf(tooSmall)).contains("Terminal size too small:");
        assertThat(textOf(recovered))
                .contains("Tapstate")
                .doesNotContain("Terminal size too small:");
    }

    @Test
    void sessionRendersTheStateOwnedByItsRuntime() {
        AtomicBoolean ownerThread = new AtomicBoolean(true);
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), callback -> callback.run(), ownerThread::get, event -> {
                });
        Workbench.Session session = new Workbench.Session(runtime);
        runtime.updateState(state -> state.select(WorkbenchState.WorkbenchTab.PIPELINES));
        Buffer buffer = Buffer.empty(new Rect(0, 0, 88, 24));

        session.render(Frame.forTesting(buffer));

        assertThat(textOf(buffer)).contains("Pipelines").contains("No pipeline snapshot is loaded yet.");
    }

    @Test
    void syntheticRedrawEventRequestsTheRunnerManagedRenderPath() {
        AtomicBoolean ownerThread = new AtomicBoolean(true);
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), callback -> callback.run(), ownerThread::get, event -> {
                });
        Workbench.Session session = new Workbench.Session(runtime);

        assertThat(session.handleEvent(WorkbenchRedrawEvent.INSTANCE, null)).isTrue();
    }

    private static String textOf(Buffer buffer) {
        StringBuilder text = new StringBuilder();
        for (int y = 0; y < buffer.height(); y++) {
            for (int x = 0; x < buffer.width(); x++) {
                String symbol = buffer.get(x, y).symbol();
                text.append(symbol.isEmpty() ? ' ' : symbol);
            }
            text.append('\n');
        }
        return text.toString();
    }
}
