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
        RealConnectorGate.require("mongodb");
    }

    @BeforeEach
    void stageTheConnector() throws IOException {
        Path jars = Files.createDirectory(directory.resolve("connectors"));
        E2eConnectorJar.buildInto(jars);
        Files.copy(ConnectorJars.pathFor("mongodb"), jars.resolve("mongodb-connector.jar"));
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
        Envelope specification = EnvelopeParser.parse(Examples.read(WORKSPACE.resolve("spec.e2e.yml")));
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = tier.launch(storage); Endpoints files = new FileEndpoints();
                MongoEndpoints mongo = new MongoEndpoints()) {
            Map<String, String> environment = Map.of("LEFT_DIR", left.toString(), "RIGHT_DIR", right.toString(),
                    "TGT_URI", storage.masterUri());
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            HttpTierBinding binding = new HttpTierBinding(control, WORKSPACE,
                    Map.of(E2eConnectorJar.CONNECTOR_ID, files, "mongodb", mongo), environment::get);
            try {
                new E2eExecutor(binding, new FilePipelineLoader(WORKSPACE),
                        Duration.ofSeconds(60), Duration.ofMillis(200)).execute(specification);
            } catch (RuntimeException | AssertionError failure) {
                FailureScene.write(Path.of("target/failure-scenes/consolidated-" + tier.name() + ".txt"),
                        specification, binding, "consolidated_orders");
                throw failure;
            }
            assertThat(mongo.count(EndpointAddress.uri(storage.masterUri()), "orders_all"))
                    .as("six distinct rows in the managed target database, separate from both file sources")
                    .isEqualTo(6);
        }
    }
}
