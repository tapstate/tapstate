package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ContextConsoleTest {

    @Test
    void reportsUsageWhenNoInteractivePrompterIsAvailable(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("workspace"));
        Output output = new Output();

        int code = new ContextConsole(manager(home), null, workspace, output.out, output.err).run();

        assertThat(code).isEqualTo(Cli.EXIT_USAGE);
        assertThat(output.errText()).contains("cli.context-usage");
    }

    @Test
    void createsAndBindsAContextThroughTheInteractiveFlow(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("workspace"));
        ContextManager manager = manager(home);
        Output output = new Output();

        int code = run(manager, workspace, output,
                "Create a context", "dev",
                "https://one.example.com, ,https://two.example.com", "yes", "true");

        assertThat(code).isZero();
        assertThat(output.outText()).contains("created context dev")
                .contains("bound dev to " + workspace.toRealPath());
        assertThat(manager.contextBoundExactlyTo(workspace)).contains("dev");
        assertThat(manager.suggestions()).extracting(ContextManager.ContextChoice::name)
                .containsExactly("dev");
    }

    @Test
    void choosesEditsBindsAndUnbindsContexts(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("workspace"));
        ContextManager manager = manager(home);
        manager.create("dev", List.of(URI.create("https://old.example.com")), true);

        Output choose = new Output();
        assertThat(run(manager, workspace, choose, "Choose a context", "dev")).isZero();
        assertThat(choose.outText()).contains("chose context dev");

        Output edit = new Output();
        assertThat(run(manager, workspace, edit, "Edit a context", "dev", "", "n")).isZero();
        assertThat(manager.suggestions().get(0).definition().seeds())
                .containsExactly(URI.create("https://old.example.com"));
        assertThat(manager.suggestions().get(0).definition().tls().verify()).isFalse();

        Output bind = new Output();
        assertThat(run(manager, workspace, bind, "Bind context to this workspace", "dev")).isZero();
        assertThat(manager.contextBoundExactlyTo(workspace)).contains("dev");

        Output unbind = new Output();
        assertThat(run(manager, workspace, unbind, "Unbind this workspace")).isZero();
        assertThat(unbind.outText()).contains("unbound dev from " + workspace.toRealPath());
        assertThat(manager.contextBoundExactlyTo(workspace)).isEmpty();

        Output noBinding = new Output();
        assertThat(run(manager, workspace, noBinding, "Unbind this workspace")).isZero();
        assertThat(noBinding.outText()).contains("no context is bound to " + workspace.toRealPath());
    }

    @Test
    void deletesOnlyAfterBothExplicitConfirmations(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("workspace"));
        ContextManager manager = manager(home);
        manager.create("dev", List.of(URI.create("https://dev.example.com")), true);
        manager.bind(workspace, "dev");

        Output first = new Output();
        assertThat(run(manager, workspace, first, "Delete a context", "dev", "no")).isZero();
        assertThat(first.outText()).contains("kept context dev", "authRef", "binding " + workspace.toRealPath());
        assertThat(manager.suggestions()).hasSize(1);

        Output second = new Output();
        assertThat(run(manager, workspace, second, "Delete a context", "dev", "yes", "no")).isZero();
        assertThat(second.outText()).contains("kept context dev");
        assertThat(manager.suggestions()).hasSize(1);

        Output third = new Output();
        assertThat(run(manager, workspace, third, "Delete a context", "dev", "true", "true")).isZero();
        assertThat(third.outText()).contains("deleted context dev");
        assertThat(manager.suggestions()).isEmpty();
    }

    @Test
    void mapsManagerFailuresAndBadActionsToStableCliCodes(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("workspace"));
        ContextManager manager = manager(home);

        Output missing = new Output();
        assertThat(run(manager, workspace, missing, "Choose a context", "missing"))
                .isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(missing.errText()).contains("cli.context-required");

        Output badSeed = new Output();
        assertThat(run(manager, workspace, badSeed, "Create a context", "dev", "http://["))
                .isEqualTo(Cli.EXIT_USAGE);
        assertThat(badSeed.errText()).contains("cli.context-usage");

        Output unknown = new Output();
        assertThat(run(manager, workspace, unknown, "not an action")).isEqualTo(Cli.EXIT_USAGE);
        assertThat(unknown.errText()).contains("cli.context-usage");

        manager.create("dev", List.of(URI.create("https://dev.example.com")), true);
        Output duplicate = new Output();
        assertThat(run(manager, workspace, duplicate, "Create a context", "dev",
                "https://other.example.com")).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(duplicate.errText()).contains("cli.context-already-exists");
    }

    @Test
    void quitDoesNotChangeTheConfiguration(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("workspace"));
        ContextManager manager = manager(home);
        Output output = new Output();

        assertThat(run(manager, workspace, output, "Quit")).isZero();
        assertThat(manager.suggestions()).isEmpty();
        assertThat(output.outText()).isEmpty();
    }

    private static ContextManager manager(Path home) {
        return new ContextManager(ContextConfigStore.underHome(home));
    }

    private static int run(ContextManager manager, Path workspace, Output output, String... answers) {
        return new ContextConsole(manager, new ScriptedPrompter(answers), workspace, output.out, output.err).run();
    }

    private static final class Output {
        private final StringWriter outText = new StringWriter();
        private final StringWriter errText = new StringWriter();
        final PrintWriter out = new PrintWriter(outText, true);
        final PrintWriter err = new PrintWriter(errText, true);

        String outText() {
            return outText.toString();
        }

        String errText() {
            return errText.toString();
        }
    }
}
