package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a refused sign-in leaves on the machine: nothing.
 *
 * <p>The first {@code up} on an unbound workspace is where a server is named, signed in to and bound.
 * When the server refuses the password the directory is left unbound, and so is the registry of
 * servers this machine knows about - the list a person reads to answer "which server am I talking
 * to". An entry there that was never usable is noise in exactly the place somebody looks when they
 * are already confused about that.
 *
 * <p>Nothing in the declarative vocabulary reaches this: its words are about what a pipeline did, and
 * what is under test is a file under the home directory no pipeline ever touches. So it is written in
 * Java, as the admission rule provides for.
 *
 * <p>The CLI runs as its own process against a real server, and the registry is read back from the
 * file the next process would read, not from anything the run reported about itself. The home is a
 * directory of this test's own: a run that let the CLI find the real one would read and write the
 * home of whoever runs the build, and what is read back here would be somebody else's leavings.
 */
@DisplayName("a refused sign-in registers no server on this machine")
class ARefusedSignInRegistersNoServerIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    /** Where the servers this machine knows about are written, under the home the CLI was given. */
    private static final String REGISTRY = ".tapstate/config.yaml";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRefusedSignInBindsNothingAndRegistersNothing(@TempDir Path home, @TempDir Path workspace)
            throws IOException {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_refused_sign_in"))) {
            new ControlPlane(server.baseUrl()).bootstrapAndLogin(USER, PASSWORD);

            // An unbound directory, the real server named on the line, and a password the server will
            // not take: the one run in which the question is asked and the answer turns out unusable.
            CliOnce.Run up = CliOnce.runWithPassword("not-" + PASSWORD, List.of("-Duser.home=" + home),
                    "up", "--server", server.baseUrl().toString(), "--user", USER,
                    "-w", workspace.toString());

            assertThat(up.exitCode()).as(report(up)).isEqualTo(1);
            assertThat(up.stderr())
                    .as("the run stopped on the sign-in and not on something else")
                    .contains("cli.auth-login-rejected");
            assertThat(registry(home))
                    .as("a server nobody ever signed in to is not one this machine knows about")
                    .doesNotContain(server.baseUrl().toString());
            assertThat(registry(home))
                    .as("and the directory it was refused for is bound to nothing")
                    .doesNotContain(workspace.toRealPath().toString());
        }
    }

    /** The registry as the next process would read it; a run that wrote nothing leaves no file at all. */
    private static String registry(Path home) throws IOException {
        Path file = home.resolve(REGISTRY);
        return Files.exists(file) ? Files.readString(file, StandardCharsets.UTF_8) : "";
    }

    private static String report(CliOnce.Run run) {
        return "up exited " + run.exitCode() + "\nstdout:\n" + run.stdout() + "\nstderr:\n" + run.stderr();
    }
}
