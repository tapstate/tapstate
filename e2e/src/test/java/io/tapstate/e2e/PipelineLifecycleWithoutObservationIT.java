package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Suspending monitoring and removing its projection have no declarative specification words. */
@RequiresDocker
class PipelineLifecycleWithoutObservationIT {
    private static final String ID = "running_without_monitoring";
    @TempDir Path directory;

    @BeforeAll static void requireConnector() { RealConnectorGate.require("mongodb"); }

    @Test void theRunningEngineIsVisibleWhenItsMonitoringProjectionIsAbsent() throws Exception {
        var jars = Files.createDirectory(directory.resolve("connectors"));
        Files.copy(ConnectorJars.pathFor("mongodb"), jars.resolve("mongodb-connector.jar"));
        String previous = System.setProperty("tapstate.e2e.connectors-dir", jars.toString());
        try (var storage = PreparedNativeStore.create(); var data = MongoClients.create(storage.masterUri());
                var cache = MongoClients.create(storage.cacheUri()); var server = InProcessServer.startPrepared(storage)) {
            var control = new ControlPlane(server.baseUrl()); storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb", Files.readAllBytes(jars.resolve("mongodb-connector.jar")));
            data.getDatabase(storage.masterDatabase()).getCollection("orders")
                    .insertOne(new Document("_id", 1L).append("amount", 12L));
            control.apply(Map.of("input.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: input
                    connector: mongodb
                    config: { isUri: true, uri: "%s" }
                    mode: cdc
                    tables: [orders]
                    srs: { enabled: false }
                    """.formatted(storage.masterUri())));
            control.discoverSchema("input", "mongodb", Map.of("isUri", true, "uri", storage.masterUri()));
            control.createPipelineDraft(ID, Map.of("pipelineId", ID, "mode", "wizard", "name", "Running engine", "wizard", Map.of(
                    "root", Map.of("id", "root", "sourceId", "input", "table", "orders", "key", List.of("_id"), "preTransforms", List.of()),
                    "related", List.of(), "transforms", List.of(), "output", Map.of("kind", "source", "config", Map.of(
                            "sourceId", "views", "table", "order_views", "writeMode", "upsert", "onFullLoad", "append")))));
            control.sourceRequest("POST", "/api/pipelines/" + ID + "/draft:preview", Map.of("revision", 1), null, 200);
            var publication = control.sourceRequest("POST", "/api/pipelines/" + ID + "/draft:publish", Map.of("revision", 1), null, 200);
            assertThat(((Map<?, ?>) JsonReader.parse(publication.body())).get("published")).isEqualTo(true);
            control.lifecycle(ID, LifecycleVerb.START);
            Await.until("the pipeline to run and deliver its first document", () ->
                    control.state(ID).filter(PipelineState.RUNNING::equals).isPresent()
                            && data.getDatabase(storage.masterDatabase()).getCollection("order_views").countDocuments() == 1,
                    () -> String.valueOf(control.pipelineStatus(ID)));
            assertThat(server.stopPublishingObservations()).isPositive();
            var observations = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(cache.getDatabase(storage.cacheDatabase()));
            assertThat(observations.deleteMany(new Document("pipelineId", ID)).getDeletedCount()).isPositive();

            Await.until("engine lifecycle without a monitoring observation", () -> {
                var status = control.pipelineStatus(ID);
                return "RUNNING".equals(status.get("state")) && "RUNNING".equals(status.get("displayState"))
                        && !status.containsKey("observedState") && !status.containsKey("observedAt");
            }, () -> String.valueOf(control.pipelineStatus(ID)));
            var metrics = control.sourceRequest("GET", "/api/pipelines/" + ID + "/metrics", null, null, 404);
            assertThat(metrics.body()).contains("monitor.no-observation");
            var catalog = (Map<?, ?>) JsonReader.parse(control.sourceRequest("GET", "/api/pipelines", null, null, 200).body());
            var item = (Map<?, ?>) ((List<?>) catalog.get("items")).getFirst();
            assertThat(item.get("id")).isEqualTo(ID);
            var listedStatus = (Map<?, ?>) item.get("status");
            assertThat(listedStatus.get("state")).isEqualTo("RUNNING");
            assertThat(listedStatus.get("displayState")).isEqualTo("RUNNING");
        } finally {
            if (previous == null) System.clearProperty("tapstate.e2e.connectors-dir");
            else System.setProperty("tapstate.e2e.connectors-dir", previous);
        }
    }
}
