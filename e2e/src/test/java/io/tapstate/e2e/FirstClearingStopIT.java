package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** The declarative harness cannot stage a rejected first intent before runtime preparation. */
class FirstClearingStopIT {
    private static final String PIPELINE = "first_clear";
    private static final String TABLE = "orders";

    @Test
    void aClearingStopAfterARejectedFirstStartAllowsTheNextStart(@TempDir Path directory) throws Exception {
        DockerGate.require();
        try (var storage = PreparedNativeStore.create()) {
            var source = EndpointAddress.uri(Files.createDirectory(directory.resolve("source")).toString());
            var target = EndpointAddress.uri(Files.createDirectory(directory.resolve("target")).toString());
            var files = new FileEndpoints();
            files.seed(source, TABLE, SeedRows.generated(1));
            String connector = E2eConnectorJar.BROWSABLE_CONNECTOR_ID;
            byte[] jar = Files.readAllBytes(E2eConnectorJar.buildInto(directory, connector));
            var plugins = Files.createDirectory(directory.resolve("plugins"));
            try (var authoring = InProcessServer.startPrepared(storage,
                    List.of("--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + plugins))) {
                var control = login(storage, authoring);
                control.registerConnector(connector, jar);
                control.discoverSchema("clear_source", connector, source.settings());
                control.apply(Map.of("clear_source.tap.yml", sourceDefinition("clear_source", connector, source)
                                + "mode: snapshot\ntables: [orders]\n",
                        "clear_target.tap.yml", sourceDefinition("clear_target", connector, target),
                        PIPELINE + ".tap.yml", """
                                version: tapstate/v1
                                kind: pipeline
                                id: first_clear
                                source: clear_source
                                settings: { read_mode: snapshot_only }
                                serve:
                                  from: orders
                                  sync:
                                    - source: clear_target
                                """));
                assertThat(authoring.stopPublishingObservations()).isPositive();
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                recordPreparationRejection(storage);
            }
            try (var stopping = Tiers.IN_PROCESS.launch(storage, plugins)) {
                var control = login(storage, stopping);
                assertThat(control.pipelineStatus(PIPELINE).get("failure")).isInstanceOf(Map.class);
                control.stop(PIPELINE, true);
                Await.until("the production Stop worker to confirm the clear",
                        () -> "CLEARED".equals(control.pipelineStatus(PIPELINE).get("stopState")),
                        () -> control.pipelineStatus(PIPELINE).toString());
                assertCleared(control);
            }
            try (var restarted = Tiers.IN_PROCESS.launch(storage, plugins); var mongo = MongoClients.create(storage.cacheUri())) {
                var control = login(storage, restarted);
                assertCleared(control);
                var cache = mongo.getDatabase(storage.cacheDatabase());
                assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(cache).find(new Document("_id", PIPELINE)).first()).isNull();
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(
                        new Document("_id.resourceId", PIPELINE).append("_id.resourceType", "PIPELINE_ACTUATION")).first()).isNull();
                control.registerConnector(connector, jar);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the first real run to deliver the source row", () -> {
                    var status = control.pipelineStatus(PIPELINE);
                    if ("FAILED".equals(status.get("displayState"))) throw new AssertionError(status.toString());
                    return files.count(target, TABLE) == 1;
                },
                        () -> control.pipelineStatus(PIPELINE).toString());
            }
        }
    }

    private static void recordPreparationRejection(PreparedNativeStore storage) {
        var settings = new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5));
        try (var domain = MongoConfigurationDomain.open(settings, storage.clusterId())) {
            var intents = domain.intents();
            assertThat(domain.preparations().read(PIPELINE).orElseThrow().phase())
                    .isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.UNASSIGNED);
            intents.failStart(intents.pendingStart(PIPELINE).orElseThrow(), IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", PIPELINE, "field", "revision"));
        }
    }

    private static void assertCleared(ControlPlane control) {
        assertThat(control.pipelineStatus(PIPELINE)).containsEntry("displayState", "STOPPED")
                .containsEntry("stopState", "CLEARED").doesNotContainKey("failure");
        var item = control.pipelineCatalogItems(0).stream().filter(row -> PIPELINE.equals(row.get("id")))
                .findFirst().orElseThrow();
        assertThat(item.get("status")).isInstanceOfSatisfying(Map.class, status ->
                assertThat(status).containsEntry("displayState", "STOPPED").containsEntry("stopState", "CLEARED"));
    }

    private static ControlPlane login(PreparedNativeStore storage, ServerHandle server) {
        var control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        return control;
    }

    private static String sourceDefinition(String id, String connector, EndpointAddress address) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(id, connector, address.text("uri"));
    }
}
