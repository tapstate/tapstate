package io.tapstate.cli;

import io.tapstate.core.schema.SchemaNavigator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiCommandSurfaceModelTest {

    @Test
    void paletteNormalizesEntriesAndClampsSelection() {
        TuiCommandBar.Palette palette = TuiCommandBar.palette(
                Arrays.asList(" ls ", "", "pwd", "ls", null));

        assertThat(palette.entries()).containsExactly("ls", "pwd");
        assertThat(palette.selected()).isEqualTo("ls");
        assertThat(palette.move(99).selected()).isEqualTo("pwd");
        assertThat(palette.move(-99).selected()).isEqualTo("ls");
        assertThat(palette.select(1).selected()).isEqualTo("pwd");
    }

    @Test
    void completionUsesTheSharedCompleterAndRecentHistoryWithoutDuplicates() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());
        TuiCommandHistory history = new TuiCommandHistory();
        history.record("validate workspace.yaml");
        history.record("version");

        TuiCommandBar.Completion completion = TuiCommandBar.complete(
                registry.completer(), history, List.of("v"), 0);

        assertThat(completion.candidates()).containsExactly("validate", "version");
        assertThat(completion.selected()).isEqualTo("validate");
        assertThat(completion.move(1).selected()).isEqualTo("version");
    }

    @Test
    void historyStoresOnlySafeDisplayTextAndOffersRecentPrefixMatches() {
        TuiCommandHistory history = new TuiCommandHistory(3);

        history.record("auth login alice hunter2");
        history.record("login bob hunter3");
        history.record("validate workspace.yaml");
        history.record("validate other.yaml");

        assertThat(history.entries()).doesNotContain("hunter2");
        assertThat(history.entries()).doesNotContain("hunter3");
        assertThat(history.matches("validate")).containsExactly(
                "validate other.yaml", "validate workspace.yaml");
    }

    @Test
    void redactsBareLoginPasswordFromActivityProjection() {
        assertThat(TuiActivity.command("login bob hunter3"))
                .isEqualTo("login bob [redacted]");
    }

    @Test
    void projectsCommandOutputIntoASecretFreeResultPane() {
        CommandResult result = new CommandResult(true, Cli.EXIT_DIAGNOSTIC);

        TuiCommandBar.ResultPane pane = TuiCommandBar.project(
                result, "token=tok_live\nsecond line\u001b[31m");

        assertThat(pane.keepRunning()).isTrue();
        assertThat(pane.exitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(pane.success()).isFalse();
        assertThat(pane.lines()).containsExactly("token=[redacted]", "second line");
        assertThat(pane.notice()).isEqualTo("token=[redacted] second line");
        assertThat(pane.toString()).doesNotContain("tok_live");
    }

    @Test
    void ignoresInvalidCodePointsAndStripsTerminalControlsFromDisplayText() {
        TuiCommandBar.Update invalid = TuiCommandBar.accept("ls", Character.MAX_CODE_POINT + 1);

        assertThat(invalid.value()).isEqualTo("ls");
        assertThat(TuiCommandBar.safeDisplayText("ls\u001b[2J\nnext")).isEqualTo("ls next");
    }

    @Test
    void operationProjectionClassifiesStreamsAndWritesWithoutRetainingSecrets() {
        TuiOperation stream = TuiCommandBar.operationFor("logs p1 --follow --token tok_live", 7);
        TuiOperation write = TuiCommandBar.operationFor("pipeline.start p1", 8);

        assertThat(stream.kind()).isEqualTo(TuiOperation.Kind.STREAM);
        assertThat(stream.description()).doesNotContain("tok_live");
        assertThat(write.kind()).isEqualTo(TuiOperation.Kind.WRITE);
        assertThat(write.status()).isEqualTo(TuiOperation.Status.RUNNING);
    }

    @Test
    void paletteContainsSharedAndTuiCommandsWithoutDuplicates() {
        CommandRegistry registry = CommandRegistry.standard(SchemaNavigator.bundled());

        assertThat(TuiCommandBar.paletteCommands(registry))
                .contains(":ctx", ":login", "validate", "start")
                .doesNotHaveDuplicates();
    }
}
