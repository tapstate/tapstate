package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** A real replacement graph keeps serving while only its retired cold payload cannot be deleted. */
@RequiresDocker
class AFullJoinSurvivesItsRetiredPayloadCleanupFailureIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")
            .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    private static final String PIPELINE = "join_resume";
    private static final List<String> JOIN_TABLES = List.of("join_facts", "join_dimensions", "join_indexes", "join_writers");
    private static final Duration BOUND = Duration.ofSeconds(60);
    @TempDir Path directory;

    @Test @Timeout(300)
    void theOriginalFullOperationAndGenerationSurviveAStartedGraphCrashAndFailedOldDeletes() throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n");
        FileEndpoints.replaceTable(source.resolve("customers.csv"), "id,seq\n1,7\n");
        try (var storage = PreparedNativeStore.createOn(MONGO); var client = MongoClients.create(MONGO.getReplicaSetUrl());
             var files = new FileEndpoints()) {
            var cache = client.getDatabase(storage.cacheDatabase()); var config = client.getDatabase(storage.configurationDatabase());
            var admin = client.getDatabase("admin"); var generations = config.getCollection("operator_generations");
            var destination = EndpointAddress.uri(target.toString());
            Document oldScope, nextScope, operation; String operationId; boolean deleteBlocked = false;
            try {
                var first = start(storage);
                try (first) {
                    var control = new ControlPlane(first.baseUrl()); storage.loginAndAwaitBusiness(control);
                    control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
                    var resources = AJoinResumesAfterItsTargetConfirmsIT.workspace(source, target);
                    resources.compute("pipeline.tap.yml", (name, yaml) -> yaml.replace("sync: [ { id: out, source: target } ]", "sync: [ { id: out, source: target, on_full_load: clear } ]"));
                    control.apply(resources); control.discoverSchema("src", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                    control.lifecycle(PIPELINE, LifecycleVerb.START); AJoinResumesAfterItsTargetConfirmsIT.awaitJoined(files, destination, 7L);
                    Await.until("the original joined snapshots to land", BOUND,
                            () -> control.snapshotTables(PIPELINE).size() == 2 && control.snapshotTables(PIPELINE).values().stream().allMatch(value -> value.landed()),
                            () -> String.valueOf(control.snapshotTables(PIPELINE)));
                    oldScope = cache.getCollection("join_facts").find().first().get("_id", Document.class).get("placement", Document.class).get("scope", Document.class);
                    for (String table : JOIN_TABLES)
                        assertThat(cache.getCollection(table).countDocuments(at(oldScope))).as("original " + table).isPositive();
                    long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("delete"))
                                    .append("namespace", storage.cacheDatabase() + ".join_facts").append("errorCode", 13))).get("count")).longValue();
                    deleteBlocked = true;
                    control.stop(PIPELINE, true);
                    Await.until("the safe retirement to publish one replacement despite failed physical cleanup", BOUND, () -> {
                        var row = generations.find(new Document("_id", PIPELINE)).first();
                        return row != null && Long.valueOf(2).equals(row.get("stateGeneration")) && "ACTIVE".equals(row.getString("phase"))
                                && control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent();
                    }, () -> "mapping=" + generations.find(new Document("_id", PIPELINE)).first());
                    var mapping = generations.find(new Document("_id", PIPELINE)).first();
                    operationId = mapping.getString("operationId"); operation = mapping.get("stateRebuild", Document.class);
                    assertThat(operationId).isNotBlank(); assertThat(operation.get("result", Document.class)).containsEntry("phase", "COMPLETE");
                    var retirement = cache.getCollection("operator_cleanup").find(new Document("_id", oldScope)).first();
                    assertThat(retirement.get("retirement", Document.class)).containsEntry("phase", "RETIRED").containsEntry("operationId", operationId);
                    control.lifecycle(PIPELINE, LifecycleVerb.START);
                    Await.until("the replacement graph's real snapshots to be confirmed", BOUND,
                            () -> cache.getCollection("snapshot_counts").countDocuments(new Document("_id.scope.pipelineId", PIPELINE)
                                    .append("_id.scope.stateGeneration", 2L).append("rows", new Document("$gt", 0))) == 2,
                            () -> String.valueOf(control.snapshotTables(PIPELINE)));
                    Await.until("the replacement graph's snapshot readings to be published", BOUND,
                            () -> control.snapshotRowsRead(PIPELINE).keySet().containsAll(List.of("orders", "customers")),
                            () -> String.valueOf(control.snapshotRowsRead(PIPELINE)));
                    assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry("orders", 2L).containsEntry("customers", 1L);
                    AJoinResumesAfterItsTargetConfirmsIT.awaitJoined(files, destination, 7L);
                    nextScope = cache.getCollection("join_facts").find(new Document("_id.placement.scope.stateGeneration", 2L)).first()
                            .get("_id", Document.class).get("placement", Document.class).get("scope", Document.class);
                    admin.runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", entered + 1).append("maxTimeMS", 15000));
                    Await.until("the actual retired delete failure to remain pending", BOUND,
                            () -> first.tail().contains("Retired payload cleanup remains pending (pipeline=" + PIPELINE + ", generation=1,"), first::tail);
                    assertThat(cache.getCollection("join_facts").countDocuments(at(oldScope))).isPositive();
                    FileEndpoints.replaceTable(source.resolve("customers.csv"), "id,seq\n1,7\n2,8\n");
                    FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n12,2\n");
                    awaitOrder(files, destination, 12, 8, 3);
                    FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n12,2\n13,4\n");
                    Document currentScope = nextScope;
                    Await.until("the unmatched original fact and its reverse index to become durable", BOUND,
                            () -> cache.getCollection("join_facts").countDocuments(at(currentScope)) == 4
                                    && cache.getCollection("join_indexes").countDocuments(at(currentScope)) >= 3,
                            () -> "facts=" + cache.getCollection("join_facts").countDocuments(at(currentScope)));
                    assertThat(files.fetch(destination, "orders", Map.of("id", 13L))).isEmpty();
                    assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                    assertThat(generations.find(new Document("_id", PIPELINE)).first()).containsEntry("stateGeneration", 2L).containsEntry("operationId", operationId);
                    String boot = cache.getCollection("workload_claims").find(new Document("resourceType", "NODE_SESSION")).first().getString("ownerBootId");
                    first.kill(); assertThat(first.isAlive()).as("the exact old graph process has exited").isFalse();
                    Await.until("all old boot leases to expire by server time before replacement", BOUND, () ->
                            cache.getCollection("workload_claims").aggregate(List.of(new Document("$match", new Document("ownerBootId", boot)),
                                    new Document("$project", new Document("leased", new Document("$gt", List.of("$leaseUntil", "$$NOW"))))))
                                    .into(new ArrayList<>()).stream().noneMatch(row -> Boolean.TRUE.equals(row.getBoolean("leased"))), () -> "old boot=" + boot);
                }
                try (var replacement = start(storage)) {
                    var control = new ControlPlane(replacement.baseUrl()); storage.loginAndAwaitBusiness(control);
                    Await.until("the retained replacement graph to resume without another start", BOUND,
                            () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(), () -> String.valueOf(control.state(PIPELINE)));
                    awaitOrder(files, destination, 12, 8, 3);
                    FileEndpoints.replaceTable(source.resolve("customers.csv"), "id,seq\n1,7\n2,8\n3,9\n");
                    FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n12,2\n13,4\n14,3\n");
                    awaitOrder(files, destination, 14, 9, 4);
                    assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry("orders", 0L).containsEntry("customers", 0L);
                    var resumed = generations.find(new Document("_id", PIPELINE)).first();
                    assertThat(resumed).containsEntry("stateGeneration", 2L).containsEntry("operationId", operationId).containsEntry("stateRebuild", operation);
                    assertThat(cache.getCollection("join_facts").countDocuments(at(oldScope))).isPositive();
                    assertThat(cache.getCollection("operator_cleanup").find(new Document("_id", oldScope)).first().get("retirement", Document.class))
                            .containsEntry("phase", "RETIRED").containsEntry("operationId", operationId);
                    var liveState = joinedState(cache, nextScope);
                    liveState.forEach((table, rows) -> assertThat(rows).as("live " + table + " before cleanup").isNotEmpty());
                    admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); deleteBlocked = false;
                    Await.until("only the exact old Join payload to be reclaimed after permission returns", BOUND,
                            () -> JOIN_TABLES.stream()
                                    .allMatch(table -> cache.getCollection(table).countDocuments(at(oldScope)) == 0),
                            () -> "old facts=" + cache.getCollection("join_facts").countDocuments(at(oldScope)));
                    assertThat(joinedState(cache, nextScope)).as("every live fact, dimension, index and writer record survives old cleanup").isEqualTo(liveState);
                    FileEndpoints.replaceTable(source.resolve("customers.csv"), "id,seq\n1,7\n2,8\n3,9\n4,44\n");
                    awaitOrder(files, destination, 13, 44, 5);
                    FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,1\n12,2\n13,4\n14,3\n15,1\n");
                    awaitOrder(files, destination, 15, 7, 6);
                    for (var expected : Map.of(10L, 7L, 11L, 7L, 12L, 8L, 13L, 44L, 14L, 9L, 15L, 7L).entrySet())
                        assertThat(files.fetch(destination, "orders", Map.of("id", expected.getKey())))
                                .hasValueSatisfying(row -> assertThat(((Number) row.get("seq")).longValue()).isEqualTo(expected.getValue()));
                    assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                    assertThat(generations.find(new Document("_id", PIPELINE)).first()).containsEntry("stateGeneration", 2L).containsEntry("operationId", operationId).containsEntry("stateRebuild", operation);
                }
            } finally { if (deleteBlocked) admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
        }
    }

    private RealProcessServer start(PreparedNativeStore storage) {
        return RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1", port -> List.of(
                "--tapstate.cluster.id=" + storage.clusterId(), "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--tapstate.connectors.seed-dir=" + directory.resolve("no-seeds"), "--SDK_STATUS_SENDER_ENABLED=false",
                "--logging.level.io.tapstate.app.RetiredPipelinePayloadCleanup=WARN"));
    }
    private static Document at(Document scope) { return new Document("_id.placement.scope", scope); }
    private static Map<String, List<Document>> joinedState(MongoDatabase cache, Document scope) {
        var result = new LinkedHashMap<String, List<Document>>();
        for (String table : JOIN_TABLES)
            result.put(table, cache.getCollection(table).find(at(scope)).sort(new Document("_id", 1)).into(new ArrayList<>()));
        return result;
    }
    private static void awaitOrder(FileEndpoints files, EndpointAddress target, long id, long sequence, int count) {
        Await.until("the real joined target to contain the new CDC value", BOUND,
                () -> files.count(target, "orders") == count && files.fetch(target, "orders", Map.of("id", id))
                        .filter(row -> row.get("seq") instanceof Number number && number.longValue() == sequence).isPresent(),
                () -> "order=" + files.fetch(target, "orders", Map.of("id", id)));
    }
}
