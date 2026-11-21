package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot inject a MongoDB commit acknowledgement failure. */
@RequiresDocker
class PipelineDraftCommitOutcomeIT {

    @Test
    void publishReportsAnUnconfirmedCommitThatMayHaveLanded(@TempDir Path directory) throws Exception {
        Path input = Files.createDirectory(directory.resolve("input"));
        Path output = Files.createDirectory(directory.resolve("output"));
        FileEndpoints.replaceTable(input.resolve("orders.csv"), "id\n1\n");

        try (var mongo = NetworkedMongo.startWithTestCommands();
                var storage = PreparedNativeStore.createOn(mongo);
                var client = MongoClients.create(storage.configurationUri());
                var server = InProcessServer.startPrepared(storage, List.of(
                        "--tapstate.store.mongo.uri=" + storage.configurationUri()
                                + "&appName=draft-publication&timeoutMS=5000",
                        "--tapstate.cluster.node-session-renew-interval=1m",
                        "--tapstate.cluster.node-session-ttl=3m"));
                var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(Map.of("input.tap.yml", Workspaces.cdcSourceYaml("input", input),
                    "target.tap.yml", Workspaces.targetYaml("output", output)));
            control.discoverSchema("input", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", input.toString()));

            String pipeline = "orders";
            control.createPipelineDraft(pipeline, Map.of("pipelineId", pipeline, "mode", "wizard", "name", "Orders",
                    "wizard", Map.of("root", Map.of("id", "root", "sourceId", "input", "table", "orders",
                                    "key", List.of("id"), "preTransforms", List.of()),
                            "related", List.of(), "transforms", List.of(),
                            "output", Map.of("kind", "source", "config", Map.of("sourceId", "output", "table", "orders")))));
            assertThat(control.artifact(pipeline)).isEmpty();

            String appName = "draft-publication " + MongoConnection.processTag(control.clusterMembers().getFirst().bootId());
            var admin = client.getDatabase("admin");
            // Authentication and preflight checks also commit transactions. Hold the draft write
            // so the acknowledgement fault can be armed only after those checks have completed.
            String drafts = storage.configurationDatabase() + "." + SystemCollections.CONFIG_PIPELINE_DRAFTS.collectionName();
            String draftWrite = "hangDuringBatchUpdate";
            long before = admin.runCommand(new Document("configureFailPoint", draftWrite)
                    .append("mode", "alwaysOn").append("data", new Document("nss", drafts)))
                    .get("count", Number.class).longValue();
            Map<?, ?> error;
            try {
                var pending = requests.submit(() -> control.sourceResponse("POST", "/api/pipelines/" + pipeline + "/draft:publish",
                        Map.of("revision", 1), null, "en"));
                try {
                    admin.runCommand(new Document("waitForFailPoint", draftWrite)
                            .append("timesEntered", before + 1).append("maxTimeMS", 15000));
                    // A write concern error lets the commit execute while withholding its acknowledgement.
                    admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                            .append("data", new Document("failCommands", List.of("commitTransaction")).append("appName", appName)
                                    .append("blockConnection", true).append("blockTimeMS", 100)
                                    .append("writeConcernError", new Document("code", 64).append("errmsg", "test acknowledgement failure"))));
                } finally {
                    admin.runCommand(new Document("configureFailPoint", draftWrite).append("mode", "off"));
                }
                var response = pending.get();
                assertThat(response.statusCode()).as(response.body()).isEqualTo(500);
                error = (Map<?, ?>) JsonReader.parse(response.body());
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }

            var artifact = control.artifact(pipeline).orElseThrow();
            var published = control.pipelineDraft(pipeline).orElseThrow();
            assertThat(published.get("publishedDraftRevision")).isEqualTo(1L);
            assertThat(published.get("publishedArtifactHash")).isEqualTo(artifact.contentHash());
            assertThat(error.get("code")).isEqualTo(IoError.PIPELINE_COMMIT_OUTCOME_UNKNOWN.code());
            assertThat(error.get("params")).isEqualTo(Map.of("pipeline", pipeline));
            assertThat(error.get("solution")).isEqualTo(
                    "Read the current pipeline definition and draft to verify whether the original change took effect before retrying.");
        }
    }
}
