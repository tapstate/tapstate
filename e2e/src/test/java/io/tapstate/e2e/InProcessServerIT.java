package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The in-process tier's fidelity claim: the product that answers here is the real assembly root over
 * a real store, not a hand-wired subset. What makes this tier worth having is speed, and what makes
 * it honest is that everything above it only ever speaks the product's own HTTP surface - so the
 * specification that passes here is the same one the real-process tier runs.
 */
class InProcessServerIT {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: e2e_probe
            connector: mongodb
            config: { uri: "mongodb://127.0.0.1:27017/e2e" }
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void bootsTheAssemblyRootAndAnswersItsHealthProbe() {
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_health"))) {
            assertThat(new ControlPlane(server.baseUrl()).healthy()).isTrue();
        }
    }

    @Test
    void appliedResourcesComeBackFromTheServerNotFromTheFileThatWasSent() {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            control.apply(Map.of("e2e_probe.tap.yml", SOURCE));

            assertThat(control.artifactIds()).contains("e2e_probe");
        }
    }
}
