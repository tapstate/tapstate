package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** The declarative vocabulary cannot inspect the driver commands behind a live observation. */
@RequiresDocker
class IdlePipelineObservationIT {
    private static final String PIPELINE = "idle_observation";
    private static final String TABLE = "orders";
    private static final String SOURCE = "orders_source";

    @Test
    void idleObservationsAdvanceThroughBatchedReadsAndContinueAfterCdc() throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024)
                        .withNanoCPUs(1_000_000_000L))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25")) {
            mongo.start();
            try (var storage = PreparedNativeStore.createOn(mongo);
                 var server = Tiers.IN_PROCESS.launch(storage);
                 var client = MongoClients.create(storage.configurationUri())) {
                var control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector("mongodb", ConnectorJars.managedMongoBytes());
                var sourceUri = mongo.getReplicaSetUrl("idle_source");
                var source = client.getDatabase("idle_source").getCollection(TABLE);
                var targetUri = mongo.getReplicaSetUrl("idle_target");
                var target = client.getDatabase("idle_target").getCollection(TABLE);
                source.insertOne(new Document("_id", 1L).append("customer", "first"));
                control.apply(Map.of("source.tap.yml", sourceYaml(sourceUri), "target.tap.yml", targetYaml(targetUri),
                        "pipeline.tap.yml", pipelineYaml()));
                control.discoverSchema(SOURCE, "mongodb", Map.of("isUri", true, "uri", sourceUri));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the first snapshot row to reach the native target", () -> target.countDocuments() == 1,
                        () -> "target rows=" + target.countDocuments());

                var observations = client.getDatabase(storage.cacheDatabase()).getCollection("pipeline_observation");
                Date first = awaitObservation(observations, new Date(0));
                Date warm = awaitObservation(observations, first);
                var configuration = client.getDatabase(storage.configurationDatabase());
                var envelope = new Document("$or", List.of(new Document("command.aggregate", "configuration_version"),
                        new Document("command.find", "serve_definitions")));
                configuration.runCommand(new Document("profile", 1).append("filter", envelope));
                Date sampled;
                try {
                    sampled = warm;
                    for (int i = 0; i < 4; i++) sampled = awaitObservation(observations, sampled);
                    var profile = configuration.getCollection("system.profile");
                    assertThat(profile.countDocuments(new Document("command.aggregate", "configuration_version")))
                            .as("idle publication uses the combined CONFIG end check").isGreaterThanOrEqualTo(2);
                    assertThat(profile.countDocuments(new Document("command.find", "serve_definitions")))
                            .as("warm observations do not repeatedly look up unrelated artifact types").isLessThanOrEqualTo(2);
                } finally {
                    configuration.runCommand(new Document("profile", 0).append("filter", "unset"));
                }

                source.insertOne(new Document("_id", 2L).append("customer", "second"));
                Await.until("the next CDC row to reach the same sink", () -> target.countDocuments() == 2,
                        () -> "target rows=" + target.countDocuments());
                assertThat(awaitObservation(observations, sampled)).isAfter(sampled);
                Await.until("the observation to report the sink-confirmed CDC position",
                        () -> control.durablePosition(PIPELINE, TABLE).isPresent(), () -> "position is absent");
                control.stop(PIPELINE, false);
                Await.until("the checkpoint to reach STOPPED",
                        () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                        () -> "pipeline state=" + control.state(PIPELINE));
                assertRestartDeliversCdcWithAWideCheckpoint(client.getDatabase(storage.cacheDatabase()), control, source, target);
            }
        }
    }

    private static void assertRestartDeliversCdcWithAWideCheckpoint(MongoDatabase cache, ControlPlane control,
            MongoCollection<Document> source, MongoCollection<Document> target) {
        var checkpoints = cache.getCollection("pipeline_state");
        var stateId = new Document("_id", PIPELINE);
        var claims = cache.getCollection("workload_claims");
        var claimId = new Document("_id.resourceType", "PIPELINE_ACTUATION").append("_id.resourceId", PIPELINE);
        String extra = "e2ePadding";
        try {
            assertThat(checkpoints.updateOne(stateId,
                    new Document("$set", new Document(extra, "x".repeat(16 * 1024 * 1024 - 16 * 1024)))).getMatchedCount()).isOne();
            assertThat(claims.updateOne(claimId,
                    new Document("$set", new Document(extra, "x".repeat(64 * 1024)))).getMatchedCount()).isOne();

            var answer = control.start(PIPELINE, List.of(), null);
            assertThat(answer.status()).as("START reads the individually legal checkpoint and claim: %s", answer.body()).isEqualTo(200);
            source.insertOne(new Document("_id", 3L).append("customer", "after restart while wide"));
            Await.answered("CDC after restart while both rows are still wide", Duration.ofSeconds(30),
                    () -> Optional.ofNullable(target.find(new Document("_id", 3L)).first()));
        } finally {
            var clear = new Document("$unset", new Document(extra, ""));
            checkpoints.updateOne(stateId, clear);
            claims.updateOne(claimId, clear);
        }
    }

    private static Date awaitObservation(MongoCollection<Document> observations, Date after) {
        return Await.answered("a new RUNNING observation with its snapshot count", Duration.ofSeconds(30), () -> {
            var row = observations.find(new Document("pipelineId", PIPELINE).append("state", "RUNNING")
                    .append("snapshot." + TABLE + ".landed", true)
                    .append("snapshot." + TABLE + ".rowsDone", 1L)
                    .append("observedAt", new Document("$gt", after)))
                    .sort(new Document("observedAt", -1)).projection(new Document("observedAt", 1)).first();
            return Optional.ofNullable(row).map(document -> document.getDate("observedAt"));
        });
    }

    private static String sourceYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: orders_source
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(uri);
    }

    private static String targetYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: orders_target
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                """.formatted(uri);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: idle_observation
                source: orders_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_orders, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_orders
                  sync:
                    - source: orders_target
                """;
    }
}
