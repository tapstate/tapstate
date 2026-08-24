package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CLI reaching a running product from its arguments alone.
 *
 * <p>Everything else here drives the product over HTTP, which is the right shape for a specification
 * about data crossing a pipeline. This one is about the front end itself: {@code -c} / {@code -u} and
 * {@code TAPSTATE_PASSWORD} connect and sign in before any command runs, so that a whole session's
 * work fits on one line and can be put in a script. Nothing in the declarative vocabulary reaches
 * that — its words
 * ({@code count}, {@code state}, {@code error_count}) are all about what a pipeline did, and there is
 * no word for invoking the CLI and reading what the process returned — so this is written in Java, as
 * the admission rule provides for.
 *
 * <p>The CLI runs as its own process rather than being called into. That is the whole claim: these
 * options are parsed before the command table exists, and the status the process returns is what a
 * script reads. Calling a method would test neither.
 */
@DisplayName("the CLI reaches a running server from its arguments alone")
class CliOneLineLaunchIT {

    /** Where the build wrote the classpath the CLI is launched from. */
    private static final String CLI_CLASSPATH_PROPERTY = "tapstate.e2e.cli-classpath";

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aOneLineLaunchSignsInAndRunsTheCommandAgainstTheServer() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_cli_store"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            CliRun run = runCli(PASSWORD, "-c", server.baseUrl().toString(), "-u", USER, "ls");

            // `ls` is a credentialed read: reaching an answer at all proves the connection was made and
            // the credential exchanged, both from the arguments, with no session ever opened
            assertThat(run.exitCode()).isZero();
            // A server that has never been applied to is not empty any more: it registers the state store
            // its views materialize into as it starts, so the first listing on a fresh deployment shows
            // that one resource. Asserting it here rather than loosening the check keeps this test's real
            // subject -- that a one-line launch reaches a credentialed read at all -- pinned to a
            // specific answer instead of to whatever happens to come back.
            assertThat(run.stdout()).contains("source  views");
            // and the command's own output is all that reached stdout -- the point of the form is that
            // something downstream reads it, and two lines about having connected would land there too
            assertThat(run.stdout()).doesNotContain("connected to").doesNotContain("logged in as");
        }
    }

    @Test
    void aCommandThatFailedFailsTheProcess() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_cli_fail"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            CliRun run = runCli(PASSWORD, "-c", server.baseUrl().toString(), "-u", USER,
                    "get", "no-such-artifact");

            // running one command from a script is only worth anything if its outcome is the process's
            assertThat(run.exitCode()).isNotZero();
        }
    }

    @Test
    void aLaunchThatCannotSignInStopsWithoutRunningTheCommand() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_cli_auth"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            CliRun run = runCli("wrong", "-c", server.baseUrl().toString(), "-u", USER, "ls");

            assertThat(run.exitCode()).isNotZero();
            // the reason reported is the sign-in, not a missing connection reported by the verb that
            // should never have run
            assertThat(run.stdout()).doesNotContain("no resources");
        }
    }

    /** What one CLI process produced. */
    private record CliRun(int exitCode, String stdout, String stderr) {
    }

    /** Launches the CLI as its own process on the classpath the build recorded, and waits for it. */
    private static CliRun runCli(String password, String... args) {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", cliClasspath(),
                "io.tapstate.cli.Cli"));
        command.addAll(List.of(args));
        try {
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.environment().put("TAPSTATE_PASSWORD", password);
            Process process = builder.start();
            String out = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String err = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(2, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                throw new AssertionError("the CLI did not exit; output so far:\n" + out + err);
            }
            return new CliRun(process.exitValue(), out, err);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for the CLI", e);
        }
    }

    private static String cliClasspath() {
        String file = System.getProperty(CLI_CLASSPATH_PROPERTY);
        assertThat(file)
                .as("the build must set %s so the CLI can be launched", CLI_CLASSPATH_PROPERTY)
                .isNotBlank();
        try {
            return Files.readString(Path.of(file)).trim();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
