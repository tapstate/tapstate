package io.tapstate.cli;

import io.tapstate.core.schema.SchemaNavigator;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CommandRegistryTest {

    @Test
    void sharesTheCommandTableParserAndCompletionVocabularyAcrossSurfaces() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        assertThat(registry.invocation("validate \"my workspace\"").words())
                .containsExactly("validate", "my workspace");
        assertThat(registry.invocation(List.of("help")).words()).containsExactly("help");
        assertThat(registry.completer().candidates(List.of(""), 0))
                .contains("validate", "new", "apply", "help", "exit");
        assertThat(registry.commandLine().getSubcommands()).containsKeys("validate", "new", "help", "repl")
                .doesNotContainKey("tui");
    }

    @Test
    void resultKeepsTheSurfaceLoopDecisionSeparateFromTheExitStatus() {
        CommandResult continueWithDiagnostic = new CommandResult(true, Cli.EXIT_DIAGNOSTIC);
        CommandResult quitSuccessfully = new CommandResult(false, Cli.EXIT_OK);

        assertThat(continueWithDiagnostic.keepRunning()).isTrue();
        assertThat(continueWithDiagnostic.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(quitSuccessfully.keepRunning()).isFalse();
        assertThat(quitSuccessfully.exitCode()).isZero();
    }

    @Test
    void preservesTheOneShotAndReplCommandContract() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        StringWriter output = new StringWriter();
        StringWriter diagnostics = new StringWriter();
        registry.commandLine().setOut(new PrintWriter(output));
        registry.commandLine().setErr(new PrintWriter(diagnostics));

        int oneShotExit = registry.commandLine().execute("connect");

        assertThat(oneShotExit).isEqualTo(Cli.EXIT_VERB_UNAVAILABLE);
        assertThat(diagnostics.toString()).contains("cli.repl-builtin-only").contains("connect");

        output.getBuffer().setLength(0);
        Repl repl = new Repl(registry.commandLine());
        CommandResult result = registry.dispatch(repl, registry.invocation("help"));

        assertThat(result.keepRunning()).isTrue();
        assertThat(result.exitCode()).isZero();
        assertThat(output.toString()).contains("Usage: tapstate");
    }
}
