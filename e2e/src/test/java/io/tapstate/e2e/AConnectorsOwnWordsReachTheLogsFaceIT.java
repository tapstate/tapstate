package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source that cannot be connected to says why, in its own words, in that pipeline's log tail.
 *
 * <p>A connector is the only thing that knows why its connection was refused -- the password, the
 * permission, the database that is not there. From outside it, the host can say only that a read failed
 * and name the exception class, which is the difference between being sent to fix a password and being
 * sent to read a stack trace. Both channels the contract gives a connector are driven here, because the
 * shipped connectors use both: the log on the context it is driven with, and the shared static one that
 * prints to standard output until somebody listens.
 *
 * <p><b>Two things are asserted apart, because they fail apart.</b> That the words were carried at all,
 * and that they were filed against the run they belong to. A line written but unattributed is in the
 * console and nowhere else: the operator tailing that pipeline sees nothing, while a test asking only
 * "was it logged" is satisfied. So a second pipeline runs beside it, over a source that works, and its
 * tail is asserted not to hold the other one's words -- which is what a broadcast, or a single global
 * "whatever is running now", would look like.
 *
 * <p>Read through the shipped front end rather than off the API: the tail a person reads is one hop
 * further than the document, and a line reaching the face while the command printed something else would
 * leave every reader exactly where they started.
 *
 * <p>Driven with the harness's own connector, and measured rather than assumed: the shipped connectors
 * write their own diagnoses at their own levels, and a case resting on one of them would be asserting
 * that vendor's logging habits rather than this product's carrying of them.
 */
@DisplayName("a connector's own reason for refusing reaches that pipeline's log tail")
class AConnectorsOwnWordsReachTheLogsFaceIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";
    private static final String SOURCE_PASSWORD = "the-source-password";

    private static final String REFUSED_PIPELINE = "connector_words_refused";
    private static final String WORKING_PIPELINE = "connector_words_working";

    /** What the connector writes on the log of the context it was driven with. */
    private static final String ON_ITS_OWN_LOG = "password authentication failed: no password was given";

    /** What it writes on the contract's shared static channel -- different words, so each is witnessed alone. */
    private static final String ON_THE_SHARED_CHANNEL = "the connection was refused before any row was read";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theConnectorsOwnReasonReachesTheTailOfTheRunItBelongsTo(
            @TempDir Path refusedSource, @TempDir Path workingSource, @TempDir Path target) {
        FileEndpoints.replaceTable(refusedSource.resolve("orders.csv"), "id,name\n1,one\n");
        FileEndpoints.replaceTable(workingSource.resolve("orders.csv"), "id,name\n2,two\n");

        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                SharedMongo.replicaSetUrl("connector_words_state"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin(USER, PASSWORD);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, read(E2eConnectorJar.buildInto(target)));

            Map<String, String> resources = new LinkedHashMap<>();
            // The source declares it needs a password and carries none: the shape of a credential that was
            // rotated after the schema was taken, which is why the schema is discovered with one below.
            resources.put("src_refused.tap.yml", sourceYaml("src_refused", refusedSource, false));
            resources.put("src_working.tap.yml", sourceYaml("src_working", workingSource, true));
            resources.put("tgt_file.tap.yml", targetYaml(target));
            resources.put("refused.tap.yml", pipelineYaml(REFUSED_PIPELINE, "src_refused"));
            resources.put("working.tap.yml", pipelineYaml(WORKING_PIPELINE, "src_working"));
            control.apply(resources);
            control.discoverSchema("src_refused", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", refusedSource.toString(), "password", SOURCE_PASSWORD,
                            "require_password", true));
            control.discoverSchema("src_working", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", workingSource.toString(), "password", SOURCE_PASSWORD,
                            "require_password", true));

            control.lifecycle(WORKING_PIPELINE, LifecycleVerb.START);
            control.lifecycle(REFUSED_PIPELINE, LifecycleVerb.START);

            Await.until("the refused run's tail to hold the connector's own reason", TIMEOUT,
                    () -> control.logs(REFUSED_PIPELINE).contains(ON_ITS_OWN_LOG)
                            && control.logs(REFUSED_PIPELINE).contains(ON_THE_SHARED_CHANNEL),
                    () -> control.logs(REFUSED_PIPELINE));

            CliOnce.Run refused = CliOnce.runSession(PASSWORD, "logs " + REFUSED_PIPELINE + "\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);
            assertThat(refused.exitCode())
                    .as("the session must have run; stdout was:%n%s%nstderr was:%n%s",
                            refused.stdout(), refused.stderr())
                    .isZero();
            assertThat(refused.stdout())
                    .as("the connector's own account of the refusal, on the log it was driven with")
                    .contains(ON_ITS_OWN_LOG);
            assertThat(refused.stdout())
                    .as("and the one it said on the shared channel, which prints to standard output "
                            + "until somebody carries it")
                    .contains(ON_THE_SHARED_CHANNEL);

            CliOnce.Run working = CliOnce.runSession(PASSWORD, "logs " + WORKING_PIPELINE + "\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);
            assertThat(working.stdout())
                    .as("a line belongs to the run that produced it, not to whatever else is running")
                    .doesNotContain(ON_ITS_OWN_LOG)
                    .doesNotContain(ON_THE_SHARED_CHANNEL);
        }
    }

    private static String sourceYaml(String id, Path directory, boolean withPassword) {
        String password = withPassword ? ", password: \"%s\"".formatted(SOURCE_PASSWORD) : "";
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s", require_password: true%s }
                mode: cdc
                tables: [ orders ]
                """.formatted(id, E2eConnectorJar.CONNECTOR_ID, directory, password);
    }

    private static String targetYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_file
                connector: %s
                config: { uri: "%s" }
                """.formatted(E2eConnectorJar.CONNECTOR_ID, directory);
    }

    private static String pipelineYaml(String pipelineId, String sourceId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: tgt_file
                """.formatted(pipelineId, sourceId);
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException cannotRead) {
            throw new UncheckedIOException("cannot read the connector jar", cannotRead);
        }
    }
}
