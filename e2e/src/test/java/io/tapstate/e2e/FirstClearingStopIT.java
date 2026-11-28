package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
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
    private static final String CONNECTOR = E2eConnectorJar.BROWSABLE_CONNECTOR_ID;

    private record RejectedFirstStart(Path plugins, EndpointAddress target, byte[] jar, PipelineIntentScope scope) {}

    @Test
    void aClearingStopAfterARejectedFirstStartAllowsTheNextStart(@TempDir Path directory) throws Exception {
        DockerGate.require();
        try (var storage = PreparedNativeStore.create()) {
            var rejected = rejectFirstStart(storage, directory);
            try (var stopping = Tiers.IN_PROCESS.launch(storage, rejected.plugins())) {
                var control = login(storage, stopping);
                assertThat(control.pipelineStatus(PIPELINE).get("failure")).isInstanceOf(Map.class);
                control.stop(PIPELINE, true);
                Await.until("the production Stop worker to confirm the clear",
                        () -> "CLEARED".equals(control.pipelineStatus(PIPELINE).get("stopState")),
                        () -> control.pipelineStatus(PIPELINE).toString());
                assertCleared(control);
            }
            try (var restarted = Tiers.IN_PROCESS.launch(storage, rejected.plugins()); var mongo = MongoClients.create(storage.cacheUri())) {
                var control = login(storage, restarted);
                assertCleared(control);
                var cache = mongo.getDatabase(storage.cacheDatabase());
                assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(cache).find(new Document("_id", PIPELINE)).first()).isNull();
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(
                        new Document("_id.resourceId", PIPELINE).append("_id.resourceType", "PIPELINE_ACTUATION")).first()).isNull();
                control.registerConnector(CONNECTOR, rejected.jar());
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                var files = new FileEndpoints();
                Await.until("the first real run to deliver the source row", () -> {
                    var status = control.pipelineStatus(PIPELINE);
                    if ("FAILED".equals(status.get("displayState"))) throw new AssertionError(status.toString());
                    return files.count(rejected.target(), TABLE) == 1;
                },
                        () -> control.pipelineStatus(PIPELINE).toString());
            }
        }
    }

    @Test
    void aRetainedObservationAloneLeavesTheFirstClearUnproven(@TempDir Path directory) throws Exception {
        DockerGate.require();
        try (var storage = PreparedNativeStore.create(); var mongo = MongoClients.create(storage.cacheUri());
                var configuration = openConfiguration(storage)) {
            var rejected = rejectFirstStart(storage, directory);
            var scope = rejected.scope();
            assertThat(scope.stateGeneration()).isEqualTo(1L);
            var scopeKey = new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                    .append("cacheIncarnation", scope.cacheIncarnation()).append("pipelineId", scope.pipelineId())
                    .append("stateGeneration", scope.stateGeneration());
            var key = new Document("scope", scopeKey).append("epoch", 1L).append("claimGeneration", 1L)
                    .append("executionGeneration", 1L).append("topologyRevision", 1L)
                    .append("node", "retained-node").append("boot", "retained-boot");
            var observation = new Document("_id", key).append("pipelineId", PIPELINE).append("state", "RUNNING")
                    .append("metrics", new Document()).append("snapshot", new Document())
                    .append("positions", new Document()).append("facts", List.of());
            var cache = mongo.getDatabase(storage.cacheDatabase());
            var observations = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(cache);
            observations.insertOne(observation);

            try (var stopping = Tiers.IN_PROCESS.launch(storage, rejected.plugins())) {
                var control = login(storage, stopping);
                control.stop(PIPELINE, true);
                String code = IoError.PIPELINE_CLEAR_UNPROVEN.code();
                var status = Await.answered("the first clearing Stop to settle", () -> {
                    var current = control.pipelineStatus(PIPELINE);
                    return "CLEARED".equals(current.get("stopState"))
                            || current.get("failure") instanceof Map<?, ?> failure && code.equals(failure.get("code"))
                            ? java.util.Optional.of(current) : java.util.Optional.empty();
                });
                assertThat(status).containsEntry("displayState", "FAILED")
                        .extractingByKey("failure").isInstanceOfSatisfying(Map.class,
                                failure -> assertThat(failure).containsEntry("code", code));
                assertThat(configuration.intents().load(PIPELINE).orElseThrow().desired().purgeState()).isTrue();
                assertThat(observations.find(new Document("_id", key)).first()).isEqualTo(observation);
            }
        }
    }

    private static RejectedFirstStart rejectFirstStart(PreparedNativeStore storage, Path directory) throws Exception {
        var source = EndpointAddress.uri(Files.createDirectory(directory.resolve("source")).toString());
        var target = EndpointAddress.uri(Files.createDirectory(directory.resolve("target")).toString());
        new FileEndpoints().seed(source, TABLE, SeedRows.generated(1));
        byte[] jar = Files.readAllBytes(E2eConnectorJar.buildInto(directory, CONNECTOR));
        var plugins = Files.createDirectory(directory.resolve("plugins"));
        try (var authoring = InProcessServer.startPrepared(storage,
                List.of("--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + plugins))) {
            var control = login(storage, authoring);
            control.registerConnector(CONNECTOR, jar);
            control.discoverSchema("clear_source", CONNECTOR, source.settings());
            control.apply(Map.of("clear_source.tap.yml", sourceDefinition("clear_source", CONNECTOR, source)
                            + "mode: snapshot\ntables: [orders]\n",
                    "clear_target.tap.yml", sourceDefinition("clear_target", CONNECTOR, target),
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
            try (var domain = openConfiguration(storage)) {
                var intents = domain.intents();
                assertThat(domain.preparations().read(PIPELINE).orElseThrow().phase())
                        .isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                var pending = intents.pendingStart(PIPELINE).orElseThrow();
                intents.failStart(pending, IoError.DOCUMENT_UNREADABLE, Map.of("id", PIPELINE, "field", "revision"));
                return new RejectedFirstStart(plugins, target, jar, pending.intent().scope());
            }
        }
    }

    private static MongoConfigurationDomain openConfiguration(PreparedNativeStore storage) {
        var settings = new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5));
        return MongoConfigurationDomain.open(settings, storage.clusterId());
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
