package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guided entry behind bare {@code new} and {@code new <recipe>}: the server question (asked only
 * while the workspace is unbound), the sign-in that makes the answer usable, the local development
 * stack the default answer starts when nothing is listening, the recipe picker, and the non-interactive
 * form scripts drive with {@code --yes}. The server binding and the saved session are asserted through
 * the same stores the rest of the CLI reads, rooted in a temporary home so nothing touches the real
 * one; the health probe, the sign-in, the process runner and the downloader are fakes, so no socket is
 * opened and no container is started.
 */
class NewGuidedTest {

    private static final URI DEFAULT_SERVER = URI.create("http://127.0.0.1:8080");
    private static final String BLANK = "Nothing generated - I will write it myself";

    private static final List<String> TITLES = List.of(
            "Try it with sample data",
            "Mirror one table, as it changes",
            "Mirror a table, renamed / filtered / trimmed",
            "Assemble several tables into one object",
            "Consolidate the same table from several databases",
            BLANK);

    /** A process environment carrying only the sign-in password, the way a script sets it. */
    static final UnaryOperator<String> PASSWORD_IN_ENV =
            name -> "TAPSTATE_PASSWORD".equals(name) ? "from-env" : null;

    /** Captured outcome of one one-shot CLI invocation. */
    record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    /** One guided run over a fake stack: the runner and the downloader record, the probe flips healthy on {@code up -d}. */
    static final class Fakes {
        final FakeHealthProbe probe;
        final ScriptedProcessRunner runner = new ScriptedProcessRunner();
        final LocalStackTest.FakeDownloader downloads = new LocalStackTest.FakeDownloader();
        boolean dockerOnPath = true;
        int polls = LocalStack.HEALTH_POLLS;

        Fakes(boolean listening) {
            this(new FakeHealthProbe(listening));
        }

        Fakes(FakeHealthProbe probe) {
            this.probe = probe;
            runner.answerUnlessScripted("docker compose version", LocalStackTest.COMPOSE_PRESENT);
            runner.when("docker compose up -d", () -> probe.healthy = true);
        }

        LocalStack stack(Path home, UnaryOperator<String> env) {
            return new LocalStack(home, runner, downloads, () -> dockerOnPath, probe, env, millis -> { }, polls);
        }
    }

    static Run run(Path home, Prompter prompter, Fakes fakes, UnaryOperator<String> env, String... args) {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = prompter;
        cmd.home = home;
        cmd.controlPlane = fakes.probe;
        cmd.localStack = fakes.stack(home, env);
        cmd.env = env;
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    private static Run run(Path home, Prompter prompter, Fakes fakes, String... args) {
        return run(home, prompter, fakes, name -> null, args);
    }

    private static Run run(Path home, Prompter prompter, FakeHealthProbe controlPlane, String... args) {
        return run(home, prompter, new Fakes(controlPlane), args);
    }

    private static ContextManager manager(Path home) {
        return new ContextManager(ContextConfigStore.underHome(home));
    }

    private static Path stackDir(Path home) {
        return home.resolve(".tapstate/local-stack");
    }

    /** The saved session for {@code context}, read the way every online verb reads it. */
    private static boolean signedIn(Path home, String context) {
        ContextDefinition definition = ContextConfigStore.underHome(home).load().contexts().get(context);
        return AuthFileStore.underHome(home).load(definition.authRef(), definition.id()).isPresent();
    }

    // ---- a server already listening on the default ------------------------------------------------

    @Test
    void bareNewAsksTheServerSignsInThenAsksTheRecipeAndBindsTheWorkspace(@TempDir Path home, @TempDir Path ws) {
        // an empty reply to the server question takes the default; then the sign-in; then the recipe
        ScriptedPrompter prompter = new ScriptedPrompter("", "", "pw", BLANK);
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        // the opening line says what is being built, before any question
        assertThat(r.out()).startsWith("Building a workspace: a directory of .tap.yml files you can read and edit.");
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
        // the server question, then the username (an empty reply is admin) and the masked password
        assertThat(prompter.asked).hasSize(2);
        assertThat(prompter.asked.get(0)).containsIgnoringCase("server");
        assertThat(prompter.asked.get(1)).isEqualTo("Username");
        assertThat(prompter.secretQuestions).containsExactly("Password");
        assertThat(controlPlane.probed).containsExactly(DEFAULT_SERVER);
        assertThat(controlPlane.logins).containsExactly("admin:pw");
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("local");
        assertThat(manager(home).suggestions().get(0).definition().seeds()).containsExactly(DEFAULT_SERVER);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
        assertThat(signedIn(home, "local")).isTrue();
        // the recipe picker offered the six catalog titles, in catalog order, blank last
        assertThat(prompter.offered).containsExactly(TITLES);
    }

    @Test
    void aBoundWorkspaceSkipsTheServerQuestionAndTheSignIn(@TempDir Path home, @TempDir Path ws) {
        ContextManager manager = manager(home);
        manager.create("local", List.of(DEFAULT_SERVER), true);
        manager.bind(ws, "local");
        ScriptedPrompter prompter = new ScriptedPrompter(BLANK);
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
        assertThat(prompter.asked).isEmpty();
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(controlPlane.probed).isEmpty();
        assertThat(controlPlane.logins).isEmpty();
        assertThat(prompter.offered).containsExactly(TITLES);
    }

    @Test
    void aStackFromAnEarlierRunSignsInWithItsSavedPasswordWithoutAsking(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        // as an earlier run leaves it: the .tapstate root owner-only, the way the stores demand it
        Files.createDirectories(home.resolve(".tapstate"),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        Files.createDirectories(stackDir(home));
        Files.writeString(stackDir(home).resolve(".env"),
                "TAPSTATE_ADMIN_USER=admin\nTAPSTATE_ADMIN_PASSWORD=saved-earlier-xxxxxxxxxxxx\n");
        ScriptedPrompter prompter = new ScriptedPrompter("", BLANK);
        Fakes fakes = new Fakes(true);

        Run r = run(home, prompter, fakes, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.asked).hasSize(1);
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(fakes.probe.logins).containsExactly("admin:saved-earlier-xxxxxxxxxxxx");
        assertThat(fakes.runner.calls).as("it is already running; nothing is started").isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void aRejectedSignInStopsWithTheLoginCodeAndBindsNothing(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("", "alice", "wrong", BLANK);
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);
        controlPlane.loginOutcome = new LoginOutcome.Rejected("control.auth-failed", "no");

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.auth-login-rejected").contains("alice");
        assertThat(controlPlane.logins).containsExactly("alice:wrong");
        assertThat(prompter.offered).as("the flow stops at the sign-in it could not make").isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
        assertThat(signedIn(home, "local")).isFalse();
    }

    // ---- nothing listening on the default: the local development stack ---------------------------

    @Test
    void nothingListeningOffersTheLocalStackAndEnterStartsIt(@TempDir Path home, @TempDir Path ws) throws IOException {
        // the default server, then Enter to the offer, then the recipe
        ScriptedPrompter prompter = new ScriptedPrompter("", "", BLANK);
        Fakes fakes = new Fakes(false);

        Run r = run(home, prompter, fakes, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        Path dir = stackDir(home);
        // the offer is made in one line, before the question is asked again
        assertThat(r.out()).contains("Nothing is listening on http://127.0.0.1:8080; press Enter to start a local"
                + " development stack in Docker, or type the URL of a server you already run.\n");
        assertThat(prompter.asked).hasSize(2).allSatisfy(q -> assertThat(q).containsIgnoringCase("server"));
        // compose was checked, then the stack was brought up, in the stack directory
        assertThat(fakes.runner.calls).containsExactly(
                dir + ": docker compose version",
                dir + ": docker compose up -d");
        assertThat(Files.readString(dir.resolve("docker-compose.yml"))).isEqualTo(LocalStackTest.COMPOSE_GOLDEN);
        Map<String, String> env = DotEnv.read(dir.resolve(".env"));
        assertThat(env).containsEntry("TAPSTATE_ADMIN_USER", "admin");
        assertThat(env.get("TAPSTATE_ADMIN_PASSWORD")).hasSizeGreaterThanOrEqualTo(24);
        assertThat(fakes.downloads.fetched).hasSize(3);
        // probed before the offer, then polled until the stack answered, then signed in with the generated password
        assertThat(fakes.probe.probed).containsExactly(DEFAULT_SERVER, DEFAULT_SERVER);
        assertThat(fakes.probe.logins).containsExactly("admin:" + env.get("TAPSTATE_ADMIN_PASSWORD"));
        assertThat(prompter.secretQuestions).as("the stack's admin is signed in without asking").isEmpty();
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("local");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
        assertThat(signedIn(home, "local")).isTrue();
        // where it lives and how to stop it, then the recipe question
        assertThat(r.out()).contains("local stack: " + dir + "\n"
                + "to stop it: docker compose -f " + dir + "/docker-compose.yml down\n");
        assertThat(prompter.offered).containsExactly(TITLES);
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
    }

    @Test
    void aSecondRunReusesTheEnvAndSignsInAgainWithoutAsking(@TempDir Path home, @TempDir Path ws1, @TempDir Path ws2)
            throws IOException {
        Fakes first = new Fakes(false);
        Run r1 = run(home, new ScriptedPrompter("", "", BLANK), first, "new", "-w", ws1.toString());
        assertThat(r1.code()).as(r1.all()).isZero();
        String password = DotEnv.read(stackDir(home).resolve(".env")).get("TAPSTATE_ADMIN_PASSWORD");
        ScriptedPrompter prompter = new ScriptedPrompter("", "", BLANK);
        Fakes second = new Fakes(false);

        Run r2 = run(home, prompter, second, "new", "-w", ws2.toString());

        assertThat(r2.code()).as(r2.all()).isZero();
        assertThat(DotEnv.read(stackDir(home).resolve(".env")).get("TAPSTATE_ADMIN_PASSWORD")).isEqualTo(password);
        assertThat(second.runner.calls).anySatisfy(call -> assertThat(call).endsWith("docker compose up -d"));
        assertThat(second.probe.logins).containsExactly("admin:" + password);
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws2)).contains("local");
    }

    @Test
    void typingAUrlAtTheOfferUsesThatServerInstead(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("", "https://elsewhere:8443", "", "pw", BLANK);
        Fakes fakes = new Fakes(false);
        // the typed server answers; the default never did
        fakes.probe.healthyHost = "elsewhere";

        Run r = run(home, prompter, fakes, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(fakes.probe.probed).containsExactly(DEFAULT_SERVER, URI.create("https://elsewhere:8443"));
        assertThat(fakes.runner.calls).as("no stack is started when a URL is typed at the offer").isEmpty();
        assertThat(fakes.probe.logins).containsExactly("admin:pw");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("elsewhere");
    }

    @Test
    void yesWithNothingListeningStopsWithConnectFailedAndStartsNothing(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);

        Run r = run(home, new ScriptedPrompter(), fakes, "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.connect-failed");
        assertThat(fakes.runner.calls).as("a script never starts containers without asking for them").isEmpty();
        assertThat(stackDir(home)).doesNotExist();
        assertThat(manager(home).suggestions()).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void yesWithStartLocalStartsTheStackWithoutAPrompt(@TempDir Path home, @TempDir Path ws) throws IOException {
        ScriptedPrompter prompter = new ScriptedPrompter();
        Fakes fakes = new Fakes(false);

        Run r = run(home, prompter, fakes, "new", "blank", "--yes", "--start-local", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(prompter.asked).isEmpty();
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(fakes.runner.calls).hasSize(2);
        String password = DotEnv.read(stackDir(home).resolve(".env")).get("TAPSTATE_ADMIN_PASSWORD");
        assertThat(fakes.probe.logins).containsExactly("admin:" + password);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
        assertThat(signedIn(home, "local")).isTrue();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n").endsWith(FirstRunSummary.AI_LINE + "\n");
    }

    @Test
    void withoutDockerTheStackIsRefusedWithACodeAndNothingIsWritten(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);
        fakes.dockerOnPath = false;

        Run r = run(home, new ScriptedPrompter("", "", BLANK), fakes, "new", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.docker-unavailable");
        assertThat(fakes.runner.calls).isEmpty();
        assertThat(stackDir(home)).doesNotExist();
        assertThat(manager(home).suggestions()).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void withoutComposeTheStackIsRefusedWithWhatDockerSaid(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);
        fakes.runner.answer("docker compose version",
                new ProcessRunner.Result(1, "", "docker: 'compose' is not a docker command"));

        Run r = run(home, new ScriptedPrompter("", "", BLANK), fakes, "new", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.docker-unavailable").contains("docker: 'compose' is not a docker command");
        assertThat(fakes.runner.calls).hasSize(1);
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void aStackThatNeverAnswersIsRefusedNamingItAndBindsNothing(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);
        fakes.polls = 3;
        fakes.runner.when("docker compose up -d", () -> { });

        Run r = run(home, new ScriptedPrompter("", "", BLANK), fakes, "new", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.docker-unavailable")
                .contains(stackDir(home).toString())
                .contains("docker compose -f " + stackDir(home) + "/docker-compose.yml down");
        // the first probe was the one that found nothing; the three after it were the wait
        assertThat(fakes.probe.probed).hasSize(4);
        assertThat(fakes.probe.logins).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    // ---- a typed server ------------------------------------------------------------------------------

    @Test
    void aTypedServerAsksForCredentialsSignsInThenBinds(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("https://example:9999", "alice", "s3cret", BLANK);
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.asked).containsExactly("Which Tapstate server", "Username");
        assertThat(prompter.secretQuestions).containsExactly("Password");
        assertThat(controlPlane.probed).containsExactly(URI.create("https://example:9999"));
        assertThat(controlPlane.logins).containsExactly("alice:s3cret");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("example");
        assertThat(signedIn(home, "example")).isTrue();
    }

    @Test
    void aTypedServerThatRejectsTheSignInBindsNothing(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("https://example:9999", "alice", "wrong", BLANK);
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);
        controlPlane.loginOutcome = new LoginOutcome.Rejected("control.auth-failed", "no");

        Run r = run(home, prompter, controlPlane, "new", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.auth-login-rejected");
        assertThat(prompter.offered).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void yesWithServerUserAndThePasswordInTheEnvironmentSignsInAndBinds(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(true);

        Run r = run(home, new ScriptedPrompter(), fakes, PASSWORD_IN_ENV,
                "new", "blank", "--yes", "--server", "https://example:9999", "--user", "u", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(fakes.probe.logins).containsExactly("u:from-env");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("example");
        assertThat(signedIn(home, "example")).isTrue();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n").endsWith(FirstRunSummary.AI_LINE + "\n");
    }

    @Test
    void yesWithoutAPasswordAvailableIsRefusedNamingTheVariable(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(true);

        Run r = run(home, new ScriptedPrompter(), fakes,
                "new", "blank", "--yes", "--server", "https://example:9999", "--user", "u", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("TAPSTATE_PASSWORD");
        assertThat(fakes.probe.logins).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void thePasswordInTheEnvironmentIsTakenAtATerminalTooAndTheUserFlagSkipsThatQuestion(
            @TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("", BLANK);
        Fakes fakes = new Fakes(true);

        Run r = run(home, prompter, fakes, PASSWORD_IN_ENV, "new", "--user", "bob", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.asked).hasSize(1);
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(fakes.probe.logins).containsExactly("bob:from-env");
    }

    // ---- the rest of the entry ------------------------------------------------------------------------

    @Test
    void nothingListeningReportsConnectFailedInTheJsonEnvelope(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);

        Run r = run(home, new ScriptedPrompter(), fakes, "new", "blank", "--yes", "-w", ws.toString(), "-o", "json");

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.out()).contains("\"status\": \"error\"").contains("cli.connect-failed");
        assertThat(manager(home).suggestions()).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void yesWithARecipeIdNeverPromptsAndBindsToTheDefaultServer(@TempDir Path home, @TempDir Path ws) {
        // a prompter is injected on purpose: --yes must win over it, so a recorded question is a failure
        ScriptedPrompter prompter = new ScriptedPrompter();
        Fakes fakes = new Fakes(true);

        Run r = run(home, prompter, fakes, PASSWORD_IN_ENV, "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n").endsWith(FirstRunSummary.AI_LINE + "\n");
        assertThat(prompter.asked).isEmpty();
        assertThat(prompter.offered).isEmpty();
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(fakes.probe.probed).containsExactly(DEFAULT_SERVER);
        // no --user: the default is the same admin a terminal would be offered
        assertThat(fakes.probe.logins).containsExactly("admin:from-env");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void serverFlagProbesThatUrlAndBindsUnderTheHostName(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(true);

        Run r = run(home, new ScriptedPrompter(), fakes, PASSWORD_IN_ENV,
                "new", "blank", "--yes", "--server", "https://example:9999", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n").endsWith(FirstRunSummary.AI_LINE + "\n");
        // the up line names the server the directory was just bound to, read back from the binding
        assertThat(r.out()).contains("  tapstate up  bring it to running against https://example:9999\n");
        assertThat(fakes.probe.probed).containsExactly(URI.create("https://example:9999"));
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("example");
        assertThat(manager(home).suggestions().get(0).definition().seeds())
                .containsExactly(URI.create("https://example:9999"));
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("example");
    }

    @Test
    void aRecipeIdWithoutYesAsksTheServerAndTheSignInOnly(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter("", "", "pw");
        FakeHealthProbe controlPlane = new FakeHealthProbe(true);

        Run r = run(home, prompter, controlPlane, "new", "blank", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out()).endsWith(FirstRunSummary.AI_LINE + "\n");
        assertThat(prompter.asked).containsExactly("Which Tapstate server", "Username");
        // the recipe is already answered by the positional, so the picker is not shown
        assertThat(prompter.offered).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void anUnknownRecipeIdIsAUsageErrorPointingAtList(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(true);

        Run r = run(home, new ScriptedPrompter(), fakes, "new", "nope", "--yes", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.out()).isEmpty();
        assertThat(r.err()).startsWith("new:").contains("nope").contains("--list");
        // refused before anything is probed or written
        assertThat(fakes.probe.probed).isEmpty();
        assertThat(manager(home).suggestions()).isEmpty();
    }

    @Test
    void anExistingLocalContextIsReusedNotRecreated(@TempDir Path home, @TempDir Path ws) {
        ContextManager manager = manager(home);
        manager.create("local", List.of(DEFAULT_SERVER), true);

        Run r = run(home, new ScriptedPrompter(), new Fakes(true), PASSWORD_IN_ENV,
                "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(manager(home).suggestions()).extracting(ContextManager.ContextChoice::name).containsExactly("local");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void theNewFlagsAreOnlyForTheGuidedFirstRun(@TempDir Path home, @TempDir Path ws) {
        Run local = run(home, new ScriptedPrompter(), new Fakes(true),
                "new", "--kind", "source", "--start-local", "-w", ws.toString());
        Run user = run(home, new ScriptedPrompter(), new Fakes(true),
                "new", "--kind", "source", "--user", "u", "-w", ws.toString());

        assertThat(local.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(local.err()).contains("--start-local").contains("guided first run");
        assertThat(user.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(user.err()).contains("--user").contains("guided first run");
    }
}
