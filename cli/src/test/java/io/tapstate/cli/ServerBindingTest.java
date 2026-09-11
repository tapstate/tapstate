package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Binding a workspace to a server: the question, the sign-in that makes the answer usable, the local
 * development stack the default answer starts when nothing is listening, and every refusal that leaves
 * the directory unbound. This is the whole of the CLI's contact with a server outside the verbs that
 * talk to one, and {@code up} is the only verb that reaches it.
 *
 * <p>The binding and the saved session are asserted through the same stores the rest of the CLI reads,
 * rooted in a temporary home so nothing touches the real one; the health probe, the sign-in, the
 * process runner and the downloader are fakes, so no socket is opened and no container is started.
 */
class ServerBindingTest {

    private static final URI DEFAULT_SERVER = URI.create("http://127.0.0.1:8080");

    /** A process environment carrying only the sign-in password, the way a script sets it. */
    static final UnaryOperator<String> PASSWORD_IN_ENV =
            name -> "TAPSTATE_PASSWORD".equals(name) ? "from-env" : null;

    /** One binding over a fake stack: the runner and the downloader record, the probe flips on {@code up -d}. */
    static final class Fakes {
        final FakeHealthProbe probe;
        final ScriptedProcessRunner runner = new ScriptedProcessRunner();
        final LocalStackTest.FakeDownloader downloads = new LocalStackTest.FakeDownloader();
        boolean dockerOnPath = true;
        boolean startMakesHealthy = true;
        int polls = LocalStack.HEALTH_POLLS;

        Fakes(boolean listening) {
            this(new FakeHealthProbe(listening));
        }

        Fakes(FakeHealthProbe probe) {
            this.probe = probe;
            runner.answerUnlessScripted("docker compose version", LocalStackTest.COMPOSE_PRESENT);
        }

        LocalStack stack(Path home, UnaryOperator<String> env) {
            runner.when("docker compose -p " + LocalStack.projectNameFor(home) + " up -d", () -> {
                if (startMakesHealthy) {
                    probe.healthy = true;
                }
            });
            return new LocalStack(home, runner, downloads, () -> dockerOnPath, probe, env, millis -> { }, polls);
        }
    }

    private final StringWriter prose = new StringWriter();

    private ServerBinding binding(Path home, Prompter prompter, Fakes fakes, UnaryOperator<String> env) {
        AuthService auth = new AuthService(fakes.probe, AuthFileStore.underHome(home), Clock.systemUTC());
        return new ServerBinding(manager(home), fakes.probe, auth, fakes.stack(home, env), env, prompter,
                new PrintWriter(prose, true));
    }

    private ServerBinding binding(Path home, Prompter prompter, Fakes fakes) {
        return binding(home, prompter, fakes, name -> null);
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
    void theDefaultServerIsSignedInToAndBound(@TempDir Path home, @TempDir Path ws) throws IOException {
        // an empty reply takes the default server, then the username, then the password
        ScriptedPrompter prompter = new ScriptedPrompter("", "", "pw");
        Fakes fakes = new Fakes(true);

        binding(home, prompter, fakes).bind(ws, null, false, null);

        assertThat(fakes.probe.probed).containsExactly(DEFAULT_SERVER);
        assertThat(fakes.probe.logins).containsExactly("admin:pw");
        assertThat(fakes.runner.calls).as("something was listening; nothing is started").isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
        assertThat(signedIn(home, "local")).isTrue();
    }

    @Test
    void aWorkspaceThatIsAlreadyBoundIsRecognisedAsSuch(@TempDir Path home, @TempDir Path ws) throws IOException {
        binding(home, new ScriptedPrompter("", "", "pw"), new Fakes(true)).bind(ws, null, false, null);

        assertThat(binding(home, null, new Fakes(true)).isBound(ws)).isTrue();
    }

    @Test
    void aDirectoryThatDoesNotExistYetReadsAsUnbound(@TempDir Path parent) {
        assertThat(binding(parent, null, new Fakes(true)).isBound(parent.resolve("absent"))).isFalse();
    }

    @Test
    void aRejectedSignInBindsNothing(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(true);
        fakes.probe.loginOutcome = new LoginOutcome.Rejected("auth.invalid-credentials", "no");

        assertThatThrownBy(() -> binding(home, new ScriptedPrompter("", "", "pw"), fakes).bind(ws, null, false, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.AUTH_LOGIN_REJECTED));
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void anExistingLocalContextIsReusedNotRecreated(@TempDir Path home, @TempDir Path ws1, @TempDir Path ws2)
            throws IOException {
        binding(home, new ScriptedPrompter("", "", "pw"), new Fakes(true)).bind(ws1, null, false, null);

        binding(home, new ScriptedPrompter("", "", "pw"), new Fakes(true)).bind(ws2, null, false, null);

        assertThat(manager(home).suggestions()).hasSize(1);
        assertThat(manager(home).contextBoundExactlyTo(ws2)).contains("local");
    }

    @Test
    void aSameNamedContextForAnotherServerIsNeverAdopted(@TempDir Path home, @TempDir Path ws) throws IOException {
        manager(home).create("local", List.of(URI.create("http://other.example:8080")), true);

        binding(home, new ScriptedPrompter("", "", "pw"), new Fakes(true)).bind(ws, null, false, null);

        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local-2");
        assertThat(ContextConfigStore.underHome(home).load().contexts().get("local-2").seeds())
                .containsExactly(DEFAULT_SERVER);
    }

    @Test
    void contextsForTheSameHostButDifferentPortsStaySeparate(@TempDir Path home, @TempDir Path first,
                                                             @TempDir Path second) throws IOException {
        Fakes fakes = new Fakes(true);

        binding(home, null, fakes, PASSWORD_IN_ENV).bind(first, URI.create("https://example:8080"), false, "u");
        binding(home, null, fakes, PASSWORD_IN_ENV).bind(second, URI.create("https://example:9090"), false, "u");

        assertThat(manager(home).contextBoundExactlyTo(first)).contains("https-example-8080");
        assertThat(manager(home).contextBoundExactlyTo(second)).contains("https-example-9090");
    }

    // ---- nothing listening on the default ---------------------------------------------------------

    @Test
    void nothingListeningOffersTheStackAndAnEmptyReplyStartsIt(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        // the offer, then the default again, and the stack's own admin needs no questions
        ScriptedPrompter prompter = new ScriptedPrompter("", "");
        Fakes fakes = new Fakes(false);

        binding(home, prompter, fakes).bind(ws, null, false, null);

        assertThat(prose.toString()).contains(ServerBinding.LOCAL_STACK_OFFER);
        assertThat(fakes.runner.calls).containsExactly(
                "(cwd): docker compose version",
                stackDir(home) + ": docker compose -p " + LocalStack.projectNameFor(home) + " up -d",
                stackDir(home) + ": docker compose -p " + LocalStack.projectNameFor(home) + " wait bootstrap");
        String password = DotEnv.read(stackDir(home).resolve(".env")).get("TAPSTATE_ADMIN_PASSWORD");
        assertThat(fakes.probe.logins).containsExactly("admin:" + password);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
        assertThat(prose.toString()).contains("to stop it: docker compose -p " + LocalStack.projectNameFor(home)
                + " -f " + stackDir(home));
    }

    @Test
    void typingAUrlAtTheOfferUsesThatServerInstead(@TempDir Path home, @TempDir Path ws) throws IOException {
        FakeHealthProbe probe = new FakeHealthProbe(false);
        probe.healthyHost = "example";
        Fakes fakes = new Fakes(probe);

        binding(home, new ScriptedPrompter("", "https://example:9999", "u", "pw"), fakes).bind(ws, null, false, null);

        assertThat(fakes.runner.calls).as("a typed URL starts no stack").isEmpty();
        assertThat(fakes.probe.logins).containsExactly("u:pw");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("https-example-9999");
    }

    @Test
    void aStackAnEarlierRunLeftIsSignedInToWithItsSavedPassword(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        binding(home, new ScriptedPrompter("", ""), new Fakes(false)).bind(home.resolve("first"), null, false, null);
        String password = DotEnv.read(stackDir(home).resolve(".env")).get("TAPSTATE_ADMIN_PASSWORD");
        Fakes second = new Fakes(true);

        // no prompter at all: naming the default explicitly, the saved admin answers both questions
        binding(home, null, second).bind(ws, DEFAULT_SERVER, false, null);

        assertThat(second.probe.logins).containsExactly("admin:" + password);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void aRejectedSavedAdminFallsBackToCredentialsForAnotherLocalServer(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        binding(home, new ScriptedPrompter("", ""), new Fakes(false)).bind(home.resolve("first"), null, false, null);
        Fakes second = new Fakes(true);
        second.probe.loginOutcome = new LoginOutcome.Rejected("auth.invalid-credentials", "no");
        ScriptedPrompter prompter = new ScriptedPrompter("", "real-admin", "real-password");

        assertThatThrownBy(() -> binding(home, prompter, second).bind(ws, null, false, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.AUTH_LOGIN_REJECTED));
        assertThat(second.probe.logins).hasSize(2).endsWith("real-admin:real-password");
        assertThat(prompter.asked).contains(ServerBinding.USERNAME_QUESTION);
        assertThat(prompter.secretQuestions).contains(ServerBinding.PASSWORD_QUESTION);
    }

    // ---- what a script gets -----------------------------------------------------------------------

    @Test
    void withoutAPrompterAndWithNothingNamedTheDefaultIsNotAdopted(@TempDir Path home, @TempDir Path ws) {
        // something IS listening on the default here: adopting it silently is exactly what must not happen
        Fakes fakes = new Fakes(true);

        assertThatThrownBy(() -> binding(home, null, fakes).bind(ws, null, false, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.SERVER_NOT_NAMED));
        assertThat(fakes.probe.logins).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void withoutAPrompterStartLocalWithNothingListeningStartsTheStack(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Fakes fakes = new Fakes(false);

        binding(home, null, fakes).bind(ws, null, true, null);

        assertThat(fakes.runner.calls).hasSize(3);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void withoutAPrompterStartLocalStartsTheStackWithoutAsking(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Fakes fakes = new Fakes(false);

        binding(home, null, fakes).bind(ws, null, true, null);

        assertThat(fakes.runner.calls).hasSize(3);
        String password = DotEnv.read(stackDir(home).resolve(".env")).get("TAPSTATE_ADMIN_PASSWORD");
        assertThat(fakes.probe.logins).containsExactly("admin:" + password);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void aTypedServerWithoutAPasswordAnywhereIsRefusedNamingTheVariable(@TempDir Path home, @TempDir Path ws) {
        FakeHealthProbe probe = new FakeHealthProbe(false);
        probe.healthyHost = "example";

        assertThatThrownBy(() -> binding(home, null, new Fakes(probe))
                .bind(ws, URI.create("https://example:9999"), false, "u"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.PASSWORD_REQUIRED))
                .satisfies(e -> assertThat(((TapstateException) e).args()).containsEntry("variable", "TAPSTATE_PASSWORD"));
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void aTypedServerTakesTheUserFlagAndThePasswordFromTheEnvironment(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        FakeHealthProbe probe = new FakeHealthProbe(false);
        probe.healthyHost = "example";
        Fakes fakes = new Fakes(probe);

        binding(home, null, fakes, PASSWORD_IN_ENV)
                .bind(ws, URI.create("https://example:9999"), false, "u");

        assertThat(fakes.probe.logins).containsExactly("u:from-env");
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("https-example-9999");
    }

    @Test
    void aTypedServerThatDoesNotAnswerIsRefusedWithoutStartingAnything(@TempDir Path home, @TempDir Path ws) {
        // nothing here can start a server somewhere else, so an unreachable typed URL is the end of it
        Fakes fakes = new Fakes(false);

        assertThatThrownBy(() -> binding(home, null, fakes)
                .bind(ws, URI.create("https://example:9999"), false, "u"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.CONNECT_FAILED));
        assertThat(fakes.runner.calls).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void theDefaultNamedExplicitlyWithNothingListeningIsRefusedRatherThanStarted(@TempDir Path home,
            @TempDir Path ws) {
        // naming the default is not the same as asking for a container: --start-local is
        Fakes fakes = new Fakes(false);

        assertThatThrownBy(() -> binding(home, null, fakes).bind(ws, DEFAULT_SERVER, false, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.CONNECT_FAILED));
        assertThat(fakes.runner.calls).isEmpty();
        assertThat(stackDir(home)).doesNotExist();
    }

    // ---- the stack's own failures -----------------------------------------------------------------

    @Test
    void aFreshStackIsHandedOverOnlyOnceItsConnectorsAreRegistered(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Fakes fakes = new Fakes(false);
        fakes.probe.connectorListsBeforeSeeded = 3;

        binding(home, null, fakes).bind(ws, null, true, null);

        // three answers still had them bundled; the fourth had them registered
        assertThat(fakes.probe.connectorLists).isEqualTo(4);
        assertThat(manager(home).contextBoundExactlyTo(ws)).contains("local");
    }

    @Test
    void aFreshStackWhoseConnectorsNeverRegisterIsRefusedNamingThem(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);
        fakes.polls = 3;
        fakes.probe.connectorsNeverSeeded = true;

        assertThatThrownBy(() -> binding(home, null, fakes).bind(ws, null, true, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE))
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .contains("mysql, mongodb, postgres"));
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void withoutDockerTheStackIsRefusedAndNothingIsWritten(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);
        fakes.dockerOnPath = false;

        assertThatThrownBy(() -> binding(home, new ScriptedPrompter("", ""), fakes).bind(ws, null, false, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE));
        assertThat(fakes.runner.calls).isEmpty();
        assertThat(stackDir(home)).doesNotExist();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void aStackThatNeverAnswersIsRefusedNamingItAndBindsNothing(@TempDir Path home, @TempDir Path ws) {
        Fakes fakes = new Fakes(false);
        fakes.polls = 3;
        fakes.startMakesHealthy = false;

        assertThatThrownBy(() -> binding(home, new ScriptedPrompter("", ""), fakes).bind(ws, null, false, null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .contains(stackDir(home).toString()));
        assertThat(fakes.probe.logins).isEmpty();
        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
    }

    @Test
    void theBindingIsWrittenLastSoAFailedSignInLeavesNoTrace(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Fakes fakes = new Fakes(true);
        fakes.probe.loginOutcome = new LoginOutcome.Rejected("auth.invalid-credentials", "no");

        assertThatThrownBy(() -> binding(home, new ScriptedPrompter("", "", "pw"), fakes).bind(ws, null, false, null))
                .isInstanceOf(TapstateException.class);

        assertThat(manager(home).contextBoundExactlyTo(ws)).isEmpty();
        assertThat(Files.exists(home.resolve(".tapstate/auth"))).isFalse();
    }
}
