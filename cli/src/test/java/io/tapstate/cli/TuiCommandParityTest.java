package io.tapstate.cli;

import io.tapstate.core.schema.SchemaNavigator;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiCommandParityTest {

    @Test
    void exposesTheSharedRegistryVocabularyAcrossOneShotReplAndTui() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        List<String> oneShot = registry.commandLine().getSubcommands().keySet().stream().toList();
        List<String> repl = registry.completer().candidates(List.of(""), 0);
        List<String> tui = TuiApp.paletteCommands(registry);

        assertThat(oneShot).contains("validate", "help", "repl").doesNotContain("tui");
        assertThat(repl).contains("validate", "help", "exit");
        assertThat(tui).contains("validate", "help", "exit", ":help", ":quit").doesNotContain("tui");
    }

    @Test
    void keepsDispatchResultsIndependentFromTheTuiRenderer() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        registry.commandLine().setOut(new PrintWriter(output));
        Repl repl = new Repl(registry.commandLine());

        CommandResult result = registry.dispatch(repl, registry.invocation("help"));
        TuiCommandBar.ResultPane pane = TuiCommandBar.project(result, output.toString());

        assertThat(result.keepRunning()).isTrue();
        assertThat(result.exitCode()).isZero();
        assertThat(pane.keepRunning()).isEqualTo(result.keepRunning());
        assertThat(pane.exitCode()).isEqualTo(result.exitCode());
        assertThat(pane.lines()).anyMatch(line -> line.contains("Usage: tapstate"));
    }
}
