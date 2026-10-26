package io.tapstate.e2e;

import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Malformed union references are diagnosed before workspace resolution or publication. */
@RequiresDocker
class WizardUnionInputValidationIT {

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aSavedWizardWithAPaddedUnionInputCannotBePreviewedOrPublished(Tiers tier) {
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse(
                System.getProperty("tapstate.e2e.mongo-image", "mongo:7.0")))) {
            String owner = System.getProperty("tapstate.e2e.resource-owner");
            if (owner != null && !owner.isBlank()) {
                mongo.withLabel("io.tapstate.issue-loop.owner", owner);
            }
            mongo.withTmpFs(Map.of("/data/db", "rw,size=512m", "/data/configdb", "rw,size=16m"));
            mongo.withCreateContainerCmdModifier(command -> command.getHostConfig()
                    .withMemory(1L << 30).withNanoCPUs(1_000_000_000L));
            mongo.start();
            try (ServerHandle server = tier.launch(mongo.getReplicaSetUrl("wizard_union_validation"))) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                String id = "wizard_union_validation";
                Map<String, Object> saved = control.createPipelineDraft(id, Map.of(
                        "pipelineId", id, "mode", "wizard", "name", "Union input validation",
                        "wizard", Map.of(
                                "root", Map.of("id", "orders", "sourceId", "crm", "table", "orders",
                                        "key", List.of("id"), "preTransforms", List.of()),
                                "related", List.of(),
                                "transforms", List.of(Map.of("id", "merge", "type", "union",
                                        "fields", Map.of("from", List.of(" crm.extra")))),
                                "output", Map.of("kind", "atlas", "config",
                                        Map.of("sourceId", "atlas", "table", "orders")))));

                for (String operation : List.of("preview", "publish")) {
                    var response = control.sourceRequest("POST", "/api/pipelines/" + id + "/draft:"
                            + operation, Map.of("revision", 1), null, 400);
                    Object body = JsonReader.parse(response.body());
                    assertThat(body).isInstanceOf(Map.class);
                    Map<?, ?> diagnostic = (Map<?, ?>) body;
                    assertThat(diagnostic.get("code")).isEqualTo(PipelineDraftError.INVALID.code());
                    assertThat(diagnostic.get("params")).isEqualTo(Map.of("id", id,
                            "reason", "wizard union input must be a source-qualified table reference"));
                    assertThat(control.artifact(id)).isEmpty();
                    var read = control.sourceRequest("GET", "/api/pipelines/" + id + "/draft", null, null, 200);
                    assertThat(JsonReader.parse(read.body())).isEqualTo(saved);
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRootUnionCannotReuseARelatedBranchInputWhenPreviewedOrPublished(Tiers tier) {
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse(
                System.getProperty("tapstate.e2e.mongo-image", "mongo:7.0")))) {
            String owner = System.getProperty("tapstate.e2e.resource-owner");
            if (owner != null && !owner.isBlank()) {
                mongo.withLabel("io.tapstate.issue-loop.owner", owner);
            }
            mongo.withTmpFs(Map.of("/data/db", "rw,size=512m", "/data/configdb", "rw,size=16m"));
            mongo.withCreateContainerCmdModifier(command -> command.getHostConfig()
                    .withMemory(1L << 30).withNanoCPUs(1_000_000_000L));
            mongo.start();
            try (ServerHandle server = tier.launch(mongo.getReplicaSetUrl("wizard_cross_branch_union"))) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                String id = "wizard_cross_branch_union";
                Map<String, Object> saved = control.createPipelineDraft(id, Map.of(
                        "pipelineId", id, "mode", "wizard", "name", "Cross-branch union validation",
                        "wizard", Map.of(
                                "root", Map.of("id", "orders", "sourceId", "crm", "table", "orders",
                                        "key", List.of("id"), "preTransforms", List.of(
                                                Map.of("id", "merge", "type", "union",
                                                        "fields", Map.of("from", List.of("crm.policies"))))),
                                "related", List.of(Map.of("id", "policy", "parentId", "orders",
                                        "sourceId", "crm", "table", "policies",
                                        "relation", Map.of("on", List.of(Map.of("parentField", "id",
                                                "childField", "order_id")), "shape", "object", "path", "policy"),
                                        "preTransforms", List.of())),
                                "transforms", List.of(),
                                "output", Map.of("kind", "atlas", "config",
                                        Map.of("sourceId", "atlas", "table", "orders")))));

                for (String operation : List.of("preview", "publish")) {
                    var response = control.sourceRequest("POST", "/api/pipelines/" + id + "/draft:"
                            + operation, Map.of("revision", 1), null, 400);
                    Object body = JsonReader.parse(response.body());
                    assertThat(body).isInstanceOf(Map.class);
                    Map<?, ?> diagnostic = (Map<?, ?>) body;
                    assertThat(diagnostic.get("code")).isEqualTo(PipelineDraftError.INVALID.code());
                    assertThat(diagnostic.get("params")).isEqualTo(Map.of("id", id,
                            "reason", "wizard union input duplicates an existing pipeline input"));
                    assertThat(control.artifact(id)).isEmpty();
                    var read = control.sourceRequest("GET", "/api/pipelines/" + id + "/draft", null, null, 200);
                    assertThat(JsonReader.parse(read.body())).isEqualTo(saved);
                }
            }
        }
    }
}
