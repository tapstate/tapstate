package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A column that appears in a source after a pipeline has copied it does not reach that pipeline because
 * somebody started it again. Accepting is what takes it.
 *
 * <p>A pipeline holds its own copy of what discovery found for each table it reads, and what it publishes
 * is worked forward from that copy - including the table its sink is created to. Re-taking the copy every
 * time the pipeline is assembled empties that of meaning: the world moving arrives the next time anybody
 * presses start, with nothing reported and nobody having accepted anything, and the pipeline then produces
 * columns its author never saw. Nothing fails when it happens - rows still cross and the target still
 * fills - so only a case that reads the target's own shape says so.
 *
 * <p><b>The target's header is the witness, and the choice is the whole difficulty of this case.</b> The
 * rows are not: a row carries whatever the connector read, so a new column arriving in the data says
 * nothing about which model the pipeline was assembled from. A header does - this connector writes the
 * resolved target model's fields when the product supplies one - and it is the observation a document
 * target cannot offer, because a document simply has no column the row did not fill.
 *
 * <p><b>Every run's write is made observable rather than assumed.</b> The target file is removed before
 * each start and the case waits for it to come back, so the header being read is the one this run wrote;
 * stopping purges the resume position so the next start snapshots again rather than tailing from where
 * the last one ended and writing nothing.
 *
 * <p><b>The third run is not decoration.</b> An assertion that the header did not move is satisfied just
 * as well by a source that never changed, and the two read identically. Accepting and watching the column
 * arrive is what proves the world moved and the product saw it.
 */
class ARediscoveredColumnIsReportedRatherThanTakenIT {

    private static final String SOURCE_ID = "src_file";
    private static final String PIPELINE_ID = "e2e_copy_stands_still";
    private static final String TABLE = "orders";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    private Path connectorJar;
    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        connectorJar = E2eConnectorJar.buildInto(connectorJars);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @Test
    void aColumnThatAppearsAfterTheCopyIsNotTakenByStartingAgain() {
        writeOrders("id,amount\n1,12\n");

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_copy_stands_still"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, read(connectorJar));
            control.apply(Map.of(
                    "src_file.tap.yml", source(),
                    "tgt_file.tap.yml", target(),
                    "pipeline.tap.yml", pipeline()));
            discover(control);

            // The first start is what takes the copy: the pipeline is assembled from it.
            runOnce(control);
            assertThat(targetHeader()).containsExactly("id", "amount");

            // The world moves: the table comes back from discovery carrying a column it did not have.
            writeOrders("id,amount,note\n1,12,hello\n");
            discover(control);

            runOnce(control);
            assertThat(targetHeader())
                    .as("starting again took the new column into the pipeline's own copy")
                    .containsExactly("id", "amount");

            control.acceptDerivedSchema(PIPELINE_ID);

            runOnce(control);
            assertThat(targetHeader())
                    .as("accepting is what takes it")
                    .containsExactly("id", "amount", "note");
        }
    }

    /** Runs the pipeline once over a target that was not there, so the file read after it is this run's. */
    private void runOnce(ControlPlane control) {
        removeTargetFile();
        control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
        Await.until("the run writes the target",
                () -> Files.exists(targetFile()),
                () -> "the target file is still absent");
        control.stop(PIPELINE_ID, true);
        Await.until("the pipeline has stopped",
                () -> control.state(PIPELINE_ID).filter(PipelineState.STOPPED::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE_ID)));
    }

    private void discover(ControlPlane control) {
        control.discoverSchema(
                SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));
    }

    /** The columns the target file declares, which is the model the sink was created to. */
    private List<String> targetHeader() {
        try {
            List<String> lines = Files.readAllLines(targetFile());
            return lines.isEmpty() ? List.of() : List.of(lines.get(0).split(",", -1));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path targetFile() {
        return targetDirectory.resolve(TABLE + ".csv");
    }

    private void removeTargetFile() {
        try {
            Files.deleteIfExists(targetFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String source() {
        return """
                version: tapstate/v1
                kind: source
                id: src_file
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(sourceDirectory);
    }

    private String target() {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_file
                connector: e2e_file
                config: { uri: "%s" }
                """.formatted(targetDirectory);
    }

    private String pipeline() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_file
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: tgt_file
                """.formatted(PIPELINE_ID);
    }

    private void writeOrders(String rows) {
        try {
            Files.writeString(sourceDirectory.resolve(TABLE + ".csv"), rows);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
