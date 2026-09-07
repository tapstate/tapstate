package io.tapstate.cli;

import dev.tamboui.backend.jline3.JLineBackend;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.MouseEvent;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.util.Map;

/**
 * The full-screen terminal owner for a bare CLI launch.
 *
 * <p>This is deliberately the only place that creates a TamboUI runner. Business verbs stay outside
 * this lifecycle and continue through Picocli's one-shot command path.
 */
final class Workbench {

    private static final int MIN_WIDTH = 88;
    private static final int MIN_HEIGHT = 24;

    private Workbench() {
    }

    /** Opens the initial workbench shell and returns after its runner has restored the terminal. */
    static int run(Repl session) {
        Terminal terminal = null;
        try {
            terminal = TerminalBuilder.builder().system(true).dumb(true).build();
            if ("dumb".equalsIgnoreCase(terminal.getType())) {
                terminal.close();
                Diagnostics.printText(session.errorOutput(), CliError.WORKBENCH_NEEDS_A_TERMINAL, Map.of());
                return Cli.EXIT_DIAGNOSTIC;
            }
            try (TuiRunner runner = createRunner(terminal)) {
                terminal = null;
                Session workbench = new Session();
                runner.run(workbench::handleEvent, workbench::render);
                return Cli.EXIT_OK;
            }
        } catch (Exception ignored) {
            closeQuietly(terminal);
            Diagnostics.printText(session.errorOutput(), CliError.WORKBENCH_UNAVAILABLE, Map.of());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    /**
     * Creates a runner with an explicit JLine backend so service discovery cannot select another
     * terminal implementation when future optional integrations appear on the classpath.
     */
    private static TuiRunner createRunner(Terminal terminal) throws Exception {
        return createRunner(new JLineBackend(terminal));
    }

    private static TuiRunner createRunner(JLineBackend backend) throws Exception {
        return TuiRunner.create(TuiConfig.builder()
                .backend(backend)
                .mouseCapture(true)
                .bracketedPaste(true)
                .build());
    }

    /** Renders from the runner-owned frame so resize changes take effect without a second terminal owner. */
    static void render(Frame frame) {
        render(frame, WorkbenchState.initial());
    }

    private static TabBar render(Frame frame, WorkbenchState state) {
        Rect area = frame.area();
        if (area.width() < MIN_WIDTH || area.height() < MIN_HEIGHT) {
            renderTooSmall(frame, area);
            return null;
        }
        return renderShell(frame, area, state);
    }

    private static void renderTooSmall(Frame frame, Rect area) {
        String title = "Terminal size too small:";
        String actual = "Width = " + area.width() + "  Height = " + area.height();
        String needed = "Needed for current config:";
        String minimum = "Width = " + MIN_WIDTH + "  Height = " + MIN_HEIGHT;
        int startY = area.y() + Math.max(0, (area.height() - 5) / 2);

        writeCentered(frame, area, startY, title, Style.EMPTY.bold());
        writeCentered(frame, area, startY + 1, actual, Style.EMPTY);
        writeCentered(frame, area, startY + 3, needed, Style.EMPTY.bold());
        writeCentered(frame, area, startY + 4, minimum, Style.EMPTY);
    }

    private static TabBar renderShell(Frame frame, Rect area, WorkbenchState state) {
        frame.buffer().setString(area.x(), area.y(), "Tapstate workbench", Style.EMPTY.bold());
        TabBar tabBar = renderTabs(frame, area, state.selectedTab());
        renderActiveTab(frame, area, state.selectedTab());
        frame.buffer().setString(area.x(), area.y() + area.height() - 1,
                "1 overview  2 pipelines  3 sources  Left/Right switch  q quit", Style.EMPTY.dim());
        return tabBar;
    }

    private static TabBar renderTabs(Frame frame, Rect area, WorkbenchState.WorkbenchTab selected) {
        int x = area.x();
        for (WorkbenchState.WorkbenchTab tab : WorkbenchState.WorkbenchTab.values()) {
            String label = tab.displayLabel();
            Style style = tab == selected ? Style.EMPTY.bold().reversed() : Style.EMPTY.dim();
            frame.buffer().setString(x, area.y() + 1, label, style);
            x += label.length();
            if (tab != WorkbenchState.WorkbenchTab.SOURCES) {
                frame.buffer().setString(x, area.y() + 1, " | ", Style.EMPTY.dim());
                x += 3;
            }
        }
        return new TabBar(area.x(), area.y() + 1);
    }

    private static void renderActiveTab(Frame frame, Rect area, WorkbenchState.WorkbenchTab selected) {
        frame.buffer().setString(area.x(), area.y() + 3, selected.label(), Style.EMPTY.bold());
        frame.buffer().setString(area.x(), area.y() + 5, selected.emptyMessage(), Style.EMPTY);
        frame.buffer().setString(area.x(), area.y() + 7,
                "Refresh, selection, and data views will be connected by the workbench runtime.", Style.EMPTY.dim());
    }

    private static void writeCentered(Frame frame, Rect area, int y, String text, Style style) {
        int x = area.x() + Math.max(0, (area.width() - text.length()) / 2);
        frame.buffer().setString(x, y, text, style);
    }

    private static void closeQuietly(Terminal terminal) {
        if (terminal == null) {
            return;
        }
        try {
            terminal.close();
        } catch (Exception ignored) {
            // The original terminal initialization failure is the diagnosable outcome.
        }
    }

    /** The tab bar hit map from the most recently rendered frame. */
    private record TabBar(int x, int y) {

        private WorkbenchState.WorkbenchTab clickedTab(MouseEvent mouse) {
            if (!mouse.isClick() || mouse.y() != y) {
                return null;
            }
            int offset = mouse.x() - x;
            if (offset < 0) {
                return null;
            }
            for (WorkbenchState.WorkbenchTab tab : WorkbenchState.WorkbenchTab.values()) {
                int labelWidth = tab.displayLabel().length();
                if (offset < labelWidth) {
                    return tab;
                }
                offset -= labelWidth;
                if (tab != WorkbenchState.WorkbenchTab.SOURCES) {
                    if (offset < 3) {
                        return null;
                    }
                    offset -= 3;
                }
            }
            return null;
        }
    }

    /** The sole mutable holder for input-derived state during one runner lifecycle. */
    private static final class Session {
        private WorkbenchState state = WorkbenchState.initial();
        private TabBar tabBar;

        private boolean handleEvent(Event event, TuiRunner runner) {
            if (event instanceof KeyEvent key) {
                if (key.isCharIgnoreCase('q') || key.isCtrlC()) {
                    runner.quit();
                    return true;
                }
                return updateState(state.reduce(key));
            }
            if (event instanceof MouseEvent mouse && tabBar != null) {
                WorkbenchState.WorkbenchTab clicked = tabBar.clickedTab(mouse);
                if (clicked != null) {
                    return updateState(state.select(clicked));
                }
            }
            return false;
        }

        private boolean updateState(WorkbenchState next) {
            if (next == state) {
                return false;
            }
            state = next;
            return true;
        }

        private void render(Frame frame) {
            tabBar = Workbench.render(frame, state);
        }
    }
}
