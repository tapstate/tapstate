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
 * {@code watch} refuses when its output is not a terminal, in a real process.
 *
 * <p>The refusal itself is already pinned by a unit test, which reaches it by handing the read loop a
 * terminal check that answers no. That test proves the branch is wired to something; it cannot prove
 * what decides in a process nobody stubbed. This one removes the stub: the CLI is launched as its own
 * process, so its output is a pipe and whatever the product actually consults is what answers. An
 * implementation whose check is inverted, or wired to a source that does not track the real stream,
 * passes there and fails here.
 *
 * <p>That gap is not hypothetical. What answers today is {@code System.console()}, which returns null
 * for a pipe on the language level this builds against — and stops doing so on later ones, where a
 * console is handed out regardless and only asking it separately tells a terminal from a redirect. The
 * day that changes, the refusal silently stops refusing, every unit test still passes, and this is the
 * test that goes red.
 *
 * <p>No seeding: the refusal is reached before any read is attempted, so what is watched need not exist.
 * That is also why this case is written against a pipe rather than a terminal — a pipe is the one
 * condition {@link ProcessBuilder} produces on its own, and it is the condition the refusal is about.
 */
@DisplayName("watch refuses to redraw into a pipe")
class WatchRefusesNonTtyIT {

    /** Where the build wrote the classpath the CLI is launched from. */
    private static final String CLI_CLASSPATH_PROPERTY = "tapstate.e2e.cli-classpath";

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** Never declared. The refusal comes before the read, so a namespace that resolves is not needed. */
    private static final String NAMESPACE = "views.orders";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aWatchWhoseOutputIsAPipeRefusesAndNamesBothAlternatives() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_watch_nontty"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            CliRun run = runCli(PASSWORD, "-c", server.baseUrl().toString(), "-u", USER,
                    "watch", NAMESPACE);

            // Refused, not degraded. An implementation that warned and redrew anyway would leave cursor
            // control bytes in the middle of whatever the reader piped this into, and report success.
            assertThat(run.exitCode())
                    .as("a refusal a script cannot read is not a refusal")
                    .isNotZero();
            // The code, not the prose: the prose is allowed to change, and a reader who has to match on
            // it has no contract. This is also what separates the refusal from the one the read would
            // have produced -- an implementation that got as far as asking fails here reporting a
            // data-browser code instead, which is exactly the mutation this case exists to catch.
            assertThat(run.stderr()).contains("cli.watch-needs-a-terminal");
            // Both alternatives, because a reader who reached for this verb wants one of them: a stream
            // that pipes, or a look that ends. A refusal naming neither leaves them nothing to type.
            assertThat(run.stderr()).contains("tail").contains("find");
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
