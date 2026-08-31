package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two versions in play meeting: a CLI process asking a running server which version it is.
 *
 * <p>The CLI and the server are separate builds installed by separate paths, and the number each one
 * reports is derived separately - the CLI's is compiled in, the server's is filtered into a resource
 * at package time. Every link of the chain between them is already held by a test with the other end
 * stubbed: the endpoint answers a known version to a Spring test, the client parses a canned body, the
 * verb renders a supplied string. None of those runs the chain. What is unproven until two processes
 * actually meet is that the number leaves the server, survives the HTTP call and arrives in what a
 * reader pastes into a bug report.
 *
 * <p>Nothing in the declarative vocabulary reaches this. Its words - {@code count}, {@code state},
 * {@code error_count} - all describe what a pipeline did with rows, and this claim is about neither a
 * pipeline nor rows, so there is no word to be missing and none to add: a specification about which
 * build is running would have to invent a whole second subject for that file. Java, as the admission
 * rule provides for.
 *
 * <p>The CLI runs as its own process, which is the point rather than an inconvenience. Calling into the
 * verb would supply the server's answer from the test instead of fetching it, and fetching it is the
 * entire claim.
 */
@DisplayName("a connected CLI reports the version the running server itself answered")
class BothVersionsInPlayAreReportedIT {

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theServerLineCarriesWhatTheServerItselfAnswered() {
        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("e2e_cli_version"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);

            // Read straight from the server first, so what the CLI prints is checked against a second,
            // independent reader of the same endpoint rather than against a constant this test holds.
            String reportedByTheServer = control.version();
            assertThat(reportedByTheServer)
                    .as("the server has to report a version at all before the CLI can relay one")
                    .isNotBlank();

            CliOnce.Run run = CliOnce.run("-c", server.baseUrl().toString(), "-u", USER, "-p", PASSWORD, "version");

            assertThat(run.exitCode()).isZero();
            // The line a reader pastes into a report. Both halves of one build, so both numbers are the
            // same one -- which is also what makes this the end-to-end shape of the rule that the version
            // is nailed down in one place: bump one of the two and this reddens.
            assertThat(run.stdout())
                    .contains("cli    " + reportedByTheServer)
                    .contains("server " + reportedByTheServer);
            // The control group for the mismatch advisory. A matched pair is the case that a comparison
            // written to warn unconditionally still passes every mismatch test it has, and fails only
            // here.
            assertThat(run.stderr()).doesNotContain("cli.version-mismatch");
        }
    }
}
