package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An append sink is refused by apply before any job exists. The declarative failure-code matcher
 * reads a running pipeline's observation, so it cannot witness this earlier validation boundary.
 */
class AnAppendOnlyTargetIsRefusedIT {

    private static final Path WORKSPACE = Path.of("src/test/resources/unwind-append");

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void appendIsRefusedBeforeAnythingIsFiledButTheSameUpsertDeclarationIsAccepted(
            Tiers tier, @TempDir Path directory) throws Exception {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        String store = SharedMongo.replicaSetUrl("append_unwind_" + tier.name().toLowerCase(Locale.ROOT));
        try (ServerHandle server = tier.launch(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            Map<String, String> resources = new LinkedHashMap<>();
            for (String file : new String[] {"src_file.tap.yml", "tgt_file.tap.yml", "pipeline.tap.yml"}) {
                resources.put(file, Examples.read(WORKSPACE.resolve(file))
                        .replace("${SRC_DIR}", directory.resolve("source").toString())
                        .replace("${TGT_DIR}", directory.resolve("target").toString()));
            }

            ControlPlane.Refusal refusal = control.applyExpectingRefusal(resources);
            assertThat(refusal.code()).isEqualTo("dsl.unwind-needs-an-upsert-target");
            assertThat(control.artifactIds()).doesNotContain("src_file", "tgt_file", "append_unwind");

            // Change only the write mode: a validator that rejects all expansions fails this half.
            resources.compute("pipeline.tap.yml", (file, yaml) -> yaml.replace("write_mode: append", "write_mode: upsert"));
            control.apply(resources);
            assertThat(control.artifactIds()).contains("src_file", "tgt_file", "append_unwind");
        }
    }
}
