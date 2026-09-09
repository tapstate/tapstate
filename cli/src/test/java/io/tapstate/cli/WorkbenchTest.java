package io.tapstate.cli;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchTest {

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
                .contains("TapState")
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
