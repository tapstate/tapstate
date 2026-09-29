package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The editor's durable DAG document is created and read back through the public API. */
class PipelineDraftAuthoringRoundTripIT {

    private static final String PIPELINE_ID = "editor_draft_round_trip";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDagDraftKeepsItsModeMetadataAndGraphAcrossAnHttpRead(Tiers tier) {
        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(
                PIPELINE_ID + "_" + tier.name().toLowerCase(Locale.ROOT)))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            Map<String, Object> graph = Map.of(
                    "nodes", java.util.List.of(Map.of(
                            "id", "source-orders",
                            "type", "source",
                            "sourceId", "orders-mysql",
                            "table", "orders",
                            "config", Map.of("connectorId", "orders-mysql"),
                            "metadata", Map.of("label", "Orders"))),
                    "edges", java.util.List.of(),
                    // JSON round-trips numeric values as doubles, so model the
                    // persisted viewport values the same way in this contract test.
                    "viewport", Map.of("x", 0.0, "y", 0.0, "zoom", 1.0));
            Map<String, Object> saved = control.createPipelineDraft(PIPELINE_ID, Map.of(
                    "pipelineId", PIPELINE_ID,
                    "mode", "dag",
                    "name", "Orders editor draft",
                    "description", "Persisted independently of compiled DSL",
                    "graph", graph));

            assertThat(saved)
                    .containsEntry("pipelineId", PIPELINE_ID)
                    .containsEntry("mode", "dag")
                    .containsEntry("name", "Orders editor draft")
                    .containsEntry("description", "Persisted independently of compiled DSL")
                    .containsEntry("revision", 1L);
            assertThat(saved.get("graph")).isEqualTo(graph);
        }
    }
}
