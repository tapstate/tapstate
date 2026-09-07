package io.tapstate.cli;

import dev.tamboui.backend.jline3.JLineBackend;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.tui.TuiConfig;
import dev.tamboui.tui.TuiRunner;
import dev.tamboui.tui.event.Event;
import dev.tamboui.tui.event.KeyEvent;
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
                runner.run(Workbench::handleEvent, Workbench::render);
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

    private static boolean handleEvent(Event event, TuiRunner runner) {
        if (event instanceof KeyEvent key && (key.isCharIgnoreCase('q') || key.isCtrlC())) {
            runner.quit();
            return true;
        }
        return false;
    }

    /** Renders from the runner-owned frame so resize changes take effect without a second terminal owner. */
    static void render(Frame frame) {
        Rect area = frame.area();
        if (area.width() < MIN_WIDTH || area.height() < MIN_HEIGHT) {
            renderTooSmall(frame, area);
            return;
        }
        renderShell(frame, area);
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

    private static void renderShell(Frame frame, Rect area) {
        frame.buffer().setString(area.x(), area.y(), "Tapstate workbench", Style.EMPTY.bold());
        frame.buffer().setString(area.x(), area.y() + 1, "Overview | Pipelines | Sources", Style.EMPTY);
        frame.buffer().setString(area.x(), area.y() + area.height() - 1, "q quit", Style.EMPTY.dim());
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
}
