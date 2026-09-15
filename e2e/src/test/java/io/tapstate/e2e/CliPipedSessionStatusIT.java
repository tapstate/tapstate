package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A session driven by a script, and the status it leaves behind.
 *
 * <p>Piping commands in is the form the quickstart uses and the form a script reaches for, and the
 * only thing such a caller can act on is the process status: it has no way to see a refusal that is
 * printed and then walked past. The session outlives the line that failed in it, so this is not the
 * one-shot claim {@link CliOneLineLaunchIT} makes -- there the failing command is the whole run,
 * while here it is followed by an {@code exit} that succeeds.
 *
 * <p>Nothing in the declarative vocabulary reaches it: its words ({@code count}, {@code state},
 * {@code error_count}, {@code failure_code}, {@code doc}, {@code dead_lettered}) all say what a
 * pipeline did, and there is no word for driving the CLI and reading what the process returned. So
 * this is written in Java, as the admission rule provides for.
 *
 * <p>Its own process, with a real file on standard input. The status is produced as the process ends
 * and read by whatever launched it; calling a method would exercise neither half of that.
 */
@DisplayName("a scripted session reports a refused command in its exit status")
class CliPipedSessionStatusIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aSessionThatRefusedACommandFailsTheProcess() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_cli_session"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            // the refusal is deliberately not the last line: a status read off the end of the session
            // would be the status of `exit`, which always succeeds
            CliOnce.Run run = CliOnce.runSession(PASSWORD, "get no-such-artifact\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);

            assertThat(run.stdout())
                    .as("the session must have run its whole script, or the status is about something else")
                    .contains("bye");
            assertThat(run.exitCode())
                    .as("stdout was:%n%s%nstderr was:%n%s", run.stdout(), run.stderr())
                    .isNotZero();
        }
    }

    @Test
    void aSessionThatRefusedNothingStillSucceeds() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_cli_session_ok"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            // the other half: without this, a session that always failed would pass the case above
            CliOnce.Run run = CliOnce.runSession(PASSWORD, "ls\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);

            assertThat(run.exitCode())
                    .as("stdout was:%n%s%nstderr was:%n%s", run.stdout(), run.stderr())
                    .isZero();
        }
    }
}
