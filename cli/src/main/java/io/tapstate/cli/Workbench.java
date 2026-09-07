package io.tapstate.cli;

import dev.tamboui.backend.jline3.JLineBackend;
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
                runner.run(Workbench::handleEvent, frame -> {
                    // The initial lifecycle shell owns only terminal setup and input exit semantics.
                });
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
