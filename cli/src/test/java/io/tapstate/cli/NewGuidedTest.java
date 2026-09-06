package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guided entry behind bare {@code new} and {@code new <recipe>}: the server question (asked only
 * while the workspace is unbound), the recipe picker, and the non-interactive form scripts drive with
 * {@code --yes}. The server binding is asserted through the same store the rest of the CLI reads,
 * rooted in a temporary home so nothing touches the real one; the health probe is a fake, so no
 * socket is opened.
 */
class NewGuidedTest {

    private static final URI DEFAULT_SERVER = URI.create("http://127.0.0.1:8080");

    private static final List<String> TITLES = List.of(
            "Try it with sample data",
            "Mirror one table, as it changes",
            "Mirror a table, renamed / filtered / trimmed",
            "Assemble several tables into one object",
            "Consolidate the same table from several databases",
            "Nothing generated - I will write it myself");

    /** Captured outcome of one one-shot CLI invocation. */
    private record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    private static Run run(Path home, Prompter prompter, ControlPlaneClient controlPlane, String... args) {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = prompter;
        cmd.contextManager = new ContextManager(ContextConfigStore.underHome(home));
        cmd.controlPlane = controlPlane;
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    private static ContextManager manager(Path home) {
        return new ContextManager(ContextConfigStore.underHome(home));
    }

    @Test
    void bareNewAsksTheServerThenTheRecipeAndBindsTheWorkspace(@TempDir Path home, @TempDir Path ws) {
        // an empty reply to the server question takes the default; the second scripted answer is the recipe
        ScriptedPrompter prompter = new ScriptedPrompter("", "Nothing generated - I will write it myself");
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.err()).isEmpty();
        // the opening line says what is being built, before any question
        assertThat(r.out()).startsWith("Building a workspace: a directory of .tap.yml files you can read and edit.");
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
        // the server question was asked, the default was probed, and only then was anything registered
        assertThat(prompter.asked).hasSize(1);
        assertThat(prompter.asked.get(0)).containsIgnoringCase("server");
        assertThat(controlPlane.probed).containsExactly(DEFAULT_SERVER);
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("local");
        assertThat(manager(home).suggestions().get(0).definition().seeds()).containsExactly(DEFAULT_SERVER);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
        // the recipe picker offered the six catalog titles, in catalog order, blank last
        assertThat(prompter.offered).containsExactly(TITLES);
    }

    @Test
    void aBoundWorkspaceSkipsTheServerQuestion(@TempDir Path home, @TempDir Path ws) {
        ContextManager manager = manager(home);
        manager.create("local", List.of(DEFAULT_SERVER), true);
        manager.bind(ws, "local");
        ScriptedPrompter prompter = new ScriptedPrompter("Nothing generated - I will write it myself");
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
        assertThat(prompter.asked).isEmpty();
        assertThat(controlPlane.probed).isEmpty();
        assertThat(prompter.offered).containsExactly(TITLES);
    }

    @Test
    void nothingListeningStopsWithConnectFailedAndWritesNoBinding(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("", "Nothing generated - I will write it myself");

        Run r = run(home, prompter, new FakeHealthProbe(false), "new", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.connect-failed");
        // the recipe question never came: the flow stops at the server it could not reach
        assertThat(prompter.offered).isEmpty();
        assertThat(manager(home).suggestions()).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void nothingListeningReportsConnectFailedInTheJsonEnvelope(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("", "Nothing generated - I will write it myself");

        Run r = run(home, prompter, new FakeHealthProbe(false), "new", "-w", ws.toString(), "-o", "json");

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.out()).contains("\"status\": \"error\"").contains("cli.connect-failed");
        assertThat(manager(home).suggestions()).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void yesWithARecipeIdNeverPromptsAndBindsToTheDefaultServer(@TempDir Path home, @TempDir Path ws) {
        // a prompter is injected on purpose: --yes must win over it, so a recorded question is a failure
        ScriptedPrompter prompter = new ScriptedPrompter();
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n").endsWith(FirstRunSummary.AI_LINE + "\n");
        assertThat(prompter.asked).isEmpty();
        assertThat(prompter.offered).isEmpty();
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(controlPlane.probed).containsExactly(DEFAULT_SERVER);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void yesWithNothingListeningStopsWithConnectFailedAndStartsNothing(@TempDir Path home, @TempDir Path ws) {
        Run r = run(home, new ScriptedPrompter(), new FakeHealthProbe(false),
                "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.connect-failed");
        assertThat(manager(home).suggestions()).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void serverFlagProbesThatUrlAndBindsUnderTheHostName(@TempDir Path home, @TempDir Path ws) {
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, new ScriptedPrompter(), controlPlane,
                "new", "blank", "--yes", "--server", "http://example:9999", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n").endsWith(FirstRunSummary.AI_LINE + "\n");
        // the up line names the server the directory was just bound to, read back from the binding
        assertThat(r.out()).contains("  tapstate up  bring it to running against http://example:9999\n");
        assertThat(controlPlane.probed).containsExactly(URI.create("http://example:9999"));
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("example");
        assertThat(manager(home).suggestions().get(0).definition().seeds())
                .containsExactly(URI.create("http://example:9999"));
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("example");
    }

    @Test
    void aRecipeIdWithoutYesAsksOnlyTheServerQuestion(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("");
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "blank", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
        assertThat(prompter.asked).hasSize(1);
        // the recipe is already answered by the positional, so the picker is not shown
        assertThat(prompter.offered).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void anUnknownRecipeIdIsAUsageErrorPointingAtList(@TempDir Path home, @TempDir Path ws) {
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, new ScriptedPrompter(), controlPlane, "new", "nope", "--yes", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.out()).isEmpty();
        assertThat(r.err()).startsWith("new:").contains("nope").contains("--list");
        // refused before anything is probed or written
        assertThat(controlPlane.probed).isEmpty();
        assertThat(manager(home).suggestions()).isEmpty();
    }

    @Test
    void anExistingLocalContextIsReusedNotRecreated(@TempDir Path home, @TempDir Path ws) {
        ContextManager manager = manager(home);
        manager.create("local", List.of(DEFAULT_SERVER), true);

        Run r = run(home, new ScriptedPrompter(), new FakeHealthProbe(true),
                "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("local");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }
}
