package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the harness to the one promise every count in every specification rests on: that an alias reaches
 * the endpoint its own resource names, and no other.
 *
 * <p>A specification says {@code count: { tgt_file.orders: 3 }} and trusts the harness to know where
 * {@code tgt_file} lives. The harness learns that by reading the resource it applied. Were that lookup to
 * collapse two aliases onto one address, a specification could count the rows the harness itself seeded
 * into the source and pass without a single row having crossed the product - every await green, the
 * product never consulted. That is the one way a whole suite of specifications can be wrong at once.
 *
 * <p>So it is checked here rather than at the end of a pipeline run. The lookup is one piece of harness
 * code shared by every specification, so a run per example proves the same thing repeatedly and proves it
 * late; and a run that never started a pipeline has no count to disagree with, which is exactly when a
 * wrong address would go unnoticed. Two aliases, two directories this test chose, two different row
 * counts, and no pipeline at all: the seeds are read back by path, not by alias, so a lookup that sent
 * both to one directory cannot survive.
 *
 * <p>One tier, deliberately. The lookup runs entirely in the harness before the product is dialled, so it
 * cannot differ by how the server was launched.
 */
class EndpointResolutionIT {

    private static final long INTO_THE_SOURCE = 4;
    private static final long INTO_THE_TARGET = 7;

    @TempDir
    private Path workspace;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void eachAliasReachesTheEndpointItsOwnResourceNames() {
        writeWorkspace();

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_endpoints_store"));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.CONNECTOR_ID, files), env());
            binding.applyResources(List.of("src_file.tap.yml", "tgt_file.tap.yml"));

            binding.seed(new TableAlias("src_file", "orders"), INTO_THE_SOURCE);
            binding.seed(new TableAlias("tgt_file", "orders"), INTO_THE_TARGET);

            // Read back by the paths this test handed out, so the harness cannot agree with itself: a
            // lookup that resolved both aliases to one directory would have the second seed overwrite the
            // first, and neither directory would hold what it was asked for.
            assertThat(files.count(EndpointAddress.uri(sourceDirectory.toString()), "orders"))
                    .isEqualTo(INTO_THE_SOURCE);
            assertThat(files.count(EndpointAddress.uri(targetDirectory.toString()), "orders"))
                    .isEqualTo(INTO_THE_TARGET);

            // And the counts a specification would read resolve the same way round.
            assertThat(binding.count(new TableAlias("src_file", "orders"))).isEqualTo(INTO_THE_SOURCE);
            assertThat(binding.count(new TableAlias("tgt_file", "orders"))).isEqualTo(INTO_THE_TARGET);
        }
    }

    /**
     * A store addressed by several settings rather than one is still reachable.
     *
     * <p>Every endpoint witnessed here so far answers on a single {@code uri}, and the harness reads that
     * one setting to find it. That is a property of the two stores it happens to drive, not of stores: a
     * JDBC endpoint carries a host, a port, a database and credentials, and no one of them is the address.
     * A harness that can only dial a {@code uri} cannot reach such a store at all, so no specification
     * could ever count rows in one - which is the whole reason the real-database witnesses are hand-written
     * Java rather than published examples.
     *
     * <p>The driver here records rather than connects: what is under test is where the harness decides to
     * dial, which it settles before any socket is opened. The connector is one no run registers, because
     * applying a resource does not require its connector to be present - so the address shape can be
     * witnessed without a database, and this test stays as cheap as the one above it.
     */
    @Test
    void anEndpointAddressedByMoreThanOneSettingIsStillReachable() {
        write("src_jdbc.tap.yml", """
                version: tapstate/v1
                kind: source
                id: src_jdbc
                connector: e2e_jdbc
                config: { host: localhost, port: 3306, database: demo }
                mode: cdc
                tables: [ orders ]
                """);

        RecordingEndpoints jdbc = new RecordingEndpoints();
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_endpoints_jdbc"));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control,
                    workspace,
                    Map.of(E2eConnectorJar.CONNECTOR_ID, files, "e2e_jdbc", jdbc),
                    env());
            binding.applyResources(List.of("src_jdbc.tap.yml"));

            binding.seed(new TableAlias("src_jdbc", "orders"), 2);

            EndpointAddress dialled = jdbc.addressDialled();
            assertThat(dialled)
                    .as("the harness reached src_jdbc, whose address is host+port+database and not a uri")
                    .isNotNull();
            // Every setting, not just the ones a uri could have carried: a driver that is handed a subset
            // has to guess the rest, and a guessed port reaches a database nobody was told about.
            assertThat(dialled.text("host")).isEqualTo("localhost");
            assertThat(dialled.text("port")).isEqualTo("3306");
            assertThat(dialled.text("database")).isEqualTo("demo");
            assertThat(dialled.resourceId())
                    .as("the address names the resource it came from, so a missing setting can say whose")
                    .isEqualTo("src_jdbc");
        }
    }

    /** Records the address it was handed, so a test can see where the harness resolved to dial. */
    private static final class RecordingEndpoints implements Endpoints {

        private EndpointAddress dialled;

        EndpointAddress addressDialled() {
            return dialled;
        }

        @Override
        public void seed(EndpointAddress address, String table, long rows) {
            dialled = address;
        }

        @Override
        public void cdc(EndpointAddress address, String table, CdcOp op, long rows) {
            dialled = address;
        }

        @Override
        public long count(EndpointAddress address, String table) {
            dialled = address;
            return 0L;
        }

        @Override
        public void close() {
        }
    }

    /** Two endpoints on one connector, distinguished only by the address each carries. */
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
}
