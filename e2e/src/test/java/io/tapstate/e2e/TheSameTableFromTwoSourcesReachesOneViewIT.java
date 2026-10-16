package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Two file databases need separate provisioned directories, which the specification cannot declare. */
class TheSameTableFromTwoSourcesReachesOneViewIT {
    private static final Path WORKSPACE = Path.of("src/test/resources/consolidated-table");

    @TempDir
    private Path directory;
    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void stageTheConnector() throws IOException {
        Path jars = Files.createDirectory(directory.resolve("connectors"));
        E2eConnectorJar.buildInto(jars);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", jars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void independentSnapshotsAndChangesLandOnceAndSurviveRestart(Tiers tier) throws IOException {
        Path left = Files.createDirectory(directory.resolve("left"));
        Path right = Files.createDirectory(directory.resolve("right"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Map<String, String> environment = Map.of("LEFT_DIR", left.toString(), "RIGHT_DIR", right.toString(),
                "TGT_DIR", target.toString());
        Envelope specification = EnvelopeParser.parse(Examples.read(WORKSPACE.resolve("spec.e2e.yml")));
        String storeUri = SharedMongo.replicaSetUrl("e2e_consolidated_" + tier.name().toLowerCase(java.util.Locale.ROOT));
        try (ServerHandle server = tier.launch(storeUri); Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(control, WORKSPACE,
                    Map.of(E2eConnectorJar.CONNECTOR_ID, files), environment::get);
            try {
                new E2eExecutor(binding, new FilePipelineLoader(WORKSPACE),
                        Duration.ofSeconds(60), Duration.ofMillis(200)).execute(specification);
            } catch (RuntimeException | AssertionError failure) {
                FailureScene.write(Path.of("target/failure-scenes/consolidated-" + tier.name() + ".txt"),
                        specification, binding, "consolidated_orders");
                throw failure;
            }
            assertThat(files.count(EndpointAddress.uri(target.toString()), "orders_all"))
                    .as("six distinct rows in the target directory, which neither source was seeded into")
                    .isEqualTo(6);
        }
    }
}
