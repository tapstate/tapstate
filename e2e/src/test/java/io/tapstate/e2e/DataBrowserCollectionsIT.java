package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The read face lists what a source's connection actually holds, not what somebody declared.
 *
 * <p>The distinction is the whole assertion. A target resource names no tables at all — the declaration
 * carries a connector and an address and nothing else — so a listing derived from the declaration would
 * be empty for it no matter what the product had written. Two collections are put there by two routes
 * that have nothing in common: one the product created itself while running a pipeline, one this test
 * wrote directly into the same address behind the product's back. Both have to come back.
 *
 * <p>Its discriminating power is low and worth stating plainly: it catches the listing not being wired
 * through to the connector at all. It does not catch a listing that reaches the connector and mangles
 * what it finds — nothing here would tell a truncated list from a complete one, since two is every
 * collection there is.
 *
 * <p>One tier. What is under test is a read that happens entirely inside the control plane and the
 * connector it drives, and no part of it differs by how the server was launched.
 */
class DataBrowserCollectionsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(250);

    private static final long SEEDED_ROWS = 4;
    private static final long EVEN_ROWS = 2;
    private static final String PIPELINE_ID = "e2e_pipeline";

    private static final List<String> WORKSPACE_FILES =
            List.of("src_file.tap.yml", "tgt_file.tap.yml", "pipeline.tap.yml");

    /** Written by the product, as the sink half of a pipeline run. */
    private static final String WRITTEN_BY_THE_PRODUCT = "orders";

    /** Written by this test straight into the same directory, declared by nobody. */
    private static final String PUT_THERE_BY_HAND = "notes";

    @TempDir
    private Path workspace;

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        E2eConnectorJar.buildInto(connectorJars);
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
    void listsBothTheCollectionTheProductWroteAndTheOneItNeverHeardOf() {
        writeWorkspace();

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_browser_collections"));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.CONNECTOR_ID, files), env());

            // Register, read, seed, discover, apply - the order the declarative runs use, and each step is
            // where it is because the one before makes it answerable. Discovery in particular is pinned
            // from both sides: the seed is what gives the source a model to report, and the pipeline's row
            // expression is refused outright unless its source was discovered before the apply.
            binding.registerConnector(E2eConnectorJar.CONNECTOR_ID);
            binding.readResources(WORKSPACE_FILES);
            binding.seed(new TableAlias("src_file", WRITTEN_BY_THE_PRODUCT), SeedRows.generated(SEEDED_ROWS));
            binding.discoverSchema("src_file");
            binding.applyResources(WORKSPACE_FILES);

            binding.drive(PIPELINE_ID, LifecycleVerb.START);
            awaitRows(binding, new TableAlias("tgt_file", WRITTEN_BY_THE_PRODUCT), EVEN_ROWS);

            // Behind the product's back and after it has written its own: a collection it was never told
            // about and never touched, in the address its declaration points at.
            putThereByHand(PUT_THERE_BY_HAND);

            assertThat(control.collections("tgt_file"))
                    .as("a listing of a target that declares no tables at all")
                    .extracting(entry -> entry.get("name"))
                    .contains(WRITTEN_BY_THE_PRODUCT, PUT_THERE_BY_HAND);
        }
    }

    /** Reads the target the way the harness does, from outside the product, until the rows are there. */
    private static void awaitRows(HttpTierBinding binding, TableAlias table, long expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() - deadline < 0) {
            last = binding.count(table);
            if (last == expected) {
                return;
            }
            sleep();
        }
        assertThat(last).as("rows the product wrote into %s", table.table()).isEqualTo(expected);
    }

    /**
     * A collection nothing in the workspace mentions, written in the format the connector reads. It carries
     * a header and one row, because a file with no header is not a table this connector can describe.
     */
    private void putThereByHand(String table) {
        FileEndpoints.replaceTable(targetDirectory.resolve(table + ".csv"), "id,note\n1,written by hand\n");
    }

    private void writeWorkspace() {
        write("src_file.tap.yml", """
                version: tapstate/v1
                kind: source
                id: src_file
                connector: e2e_file
                config: { uri: "${SRC_DIR}" }
                mode: cdc
                tables: [ orders ]
                """);
        write("tgt_file.tap.yml", """
                version: tapstate/v1
                kind: source
                id: tgt_file
                connector: e2e_file
                config: { uri: "${TGT_DIR}" }
                """);
        write("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: e2e_pipeline
                source: src_file
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: even_orders, from: [orders], type: filter, expr: "int(after.id) % 2 == 0" }
                serve:
                  from: even_orders
                  sync:
                    - source: tgt_file
                """);
    }

    private UnaryOperator<String> env() {
        return Map.of(
                "SRC_DIR", sourceDirectory.toString(),
                "TGT_DIR", targetDirectory.toString())::get;
    }

    private void write(String name, String content) {
        try {
            Files.writeString(workspace.resolve(name), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the sink to write", e);
        }
    }
}
