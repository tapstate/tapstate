package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

class TuiTerminalContractTest {

    @Test
    void refusesNonInteractiveInputBeforeOpeningTheTerminal() {
        StringWriter rendered = new StringWriter();

        int exit = TuiApp.requireInteractiveTerminal(() -> false, new PrintWriter(rendered, true));

        assertThat(exit).isEqualTo(Cli.EXIT_USAGE);
        assertThat(rendered.toString()).contains("cli.tui-requires-tty").doesNotContain("\u001b[");
    }
}
