package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A real run must be stopped before one Web deletion removes both durable representations. */
class PipelineDeletionIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void oneDeleteRemovesTheStoppedPipelineAndItsDraftFromTheCatalog(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")) {
            mongo.start();
            verifyDeletion(tier, directory, mongo);
        }
    }

    private static void verifyDeletion(Tiers tier, Path directory, MongoDBContainer mongo) throws Exception {
        try (var storage = PreparedNativeStore.createOn(mongo); ServerHandle server = tier.launch(storage)) {
            RunningPipeline running = RunningPipeline.started(server, directory, storage);
            ControlPlane control = running.control();
            String id = running.pipelineId();
            ControlPlane.StoredArtifact before = control.artifact(id).orElseThrow();
            Map<String, Object> draft = control.createPipelineDraft(id, Map.of(
                    "pipelineId", id, "mode", "dag", "name", "Pipeline deletion witness",
                    "baseArtifactHash", before.contentHash(),
                    "graph", Map.of("nodes", List.of(), "edges", List.of(),
                            "viewport", Map.of("x", 0.0, "y", 0.0, "zoom", 1.0))));
            long revision = ((Number) draft.get("revision")).longValue();

            ControlPlane.Refusal refusal = control.deletePipelineExpectingRefusal(id, before.contentHash(), revision);
            assertThat(refusal.status()).isEqualTo(409);
            assertThat(refusal.code()).isEqualTo("TS0240005");
            assertThat(control.artifact(id)).contains(before);
            assertThat(control.pipelineDraft(id)).contains(draft);
            assertThat(control.pipelineCatalogIds()).contains(id);

            running.stopAndSettle();
            assertRetiringStatus(control, storage, id, before.contentHash(), revision);
            control.deletePipeline(id, control.contentHash(id), revision);

            assertThat(control.artifact(id)).isEmpty();
            assertThat(control.pipelineDraft(id)).isEmpty();
            assertThat(control.pipelineCatalogIds()).doesNotContain(id);
        }
    }

    private static void assertRetiringStatus(ControlPlane control, PreparedNativeStore storage, String id,
            String hash, long draftRevision) {
        try (var client = MongoClients.create(storage.configurationUri())) {
            var admin = client.getDatabase("admin");
            var mappings = client.getDatabase(storage.configurationDatabase()).getCollection("operator_generations");
            var key = new Document("_id", id);
            admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn")
                    .append("data", new Document("failCommands", List.of("delete"))
                            .append("namespace", storage.configurationDatabase() + ".pipelines")
                            .append("errorCode", 13)));
            try {
                assertThatThrownBy(() -> control.deletePipeline(id, hash, draftRevision))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("expected HTTP 204");
                var pending = mappings.find(key).first();
                assertThat(pending).containsEntry("phase", "RETIRING");
                assertThat(control.artifact(id)).isPresent();
                var status = control.pipelineStatus(id);
                assertThat(status).containsEntry("desiredState", "STOPPED").doesNotContainKeys("state", "observedState");
                assertThat((List<?>) status.get("connectorVersions")).isEmpty();
                assertThat(mappings.find(key).first()).isEqualTo(pending);
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }
}
