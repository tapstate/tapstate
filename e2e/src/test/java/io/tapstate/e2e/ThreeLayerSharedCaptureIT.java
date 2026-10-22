package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Two real Mongo consumers retain one native capture while one explicitly replaces its own state. */
@RequiresDocker
class ThreeLayerSharedCaptureIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    @Container private static final MongoDBContainer DATA = mongo()
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
    private static final Duration BOUND = Duration.ofSeconds(90);
    @TempDir Path work;

    @BeforeAll static void requireConnector() { RealConnectorGate.require("mongodb"); }

    @Test @Timeout(300)
    void oneExplicitFullKeepsTheOtherConsumersLiveCdcModelsAndOriginalGeneration() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "shared-" + suffix, cacheDb = "shared_cache_" + suffix, sourceDb = "shared_source_" + suffix;
        String configDb = "shared_cfg_" + suffix, configUri = CONFIG.getReplicaSetUrl(configDb);
        String sourceUri = DATA.getReplicaSetUrl(sourceDb), targetAUri = DATA.getReplicaSetUrl("shared_a_" + suffix),
                targetBUri = DATA.getReplicaSetUrl("shared_b_" + suffix), appName = "shared-source-" + suffix;
        String taggedSource = sourceUri + (sourceUri.contains("?") ? "&" : "?") + "appName=" + appName;
        NativeMongoStorageFixture.prepare(configUri, new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb), cacheDb, null), targetAUri, cluster);
        var source = EndpointAddress.uri(sourceUri); var targetA = EndpointAddress.uri(targetAUri); var targetB = EndpointAddress.uri(targetBUri);
        try (var mongo = new MongoEndpoints(); var dataClient = MongoClients.create(DATA.getReplicaSetUrl());
             var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl()); var configClient = MongoClients.create(CONFIG.getReplicaSetUrl())) {
            mongo.seed(source, "orders", SeedRows.generated(8));
            try (var server = RealProcessServer.startPrepared(configUri, "127.0.0.1", port -> List.of(
                    "--tapstate.cluster.id=" + cluster, "--tapstate.cluster.node-id=shared-node", "--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds")))) {
                var control = new ControlPlane(server.baseUrl()); control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                var resources = new LinkedHashMap<String, String>();
                resources.put("views.tap.yml", control.artifact("views").orElseThrow().canonicalForm());
                resources.put("input.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: input
                        connector: mongodb
                        config: { isUri: true, uri: "%s" }
                        mode: cdc
                        tables: [ orders ]
                        """.formatted(taggedSource));
                resources.put("target_b.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: target_b
                        connector: mongodb
                        config: { isUri: true, uri: "%s" }
                        """.formatted(targetBUri));
                resources.put("reader_a.tap.yml", pipeline("reader_a", "views"));
                resources.put("reader_b.tap.yml", pipeline("reader_b", "target_b"));
                control.apply(resources); control.discoverSchema("input", "mongodb", Map.of("isUri", true, "uri", taggedSource));
                control.lifecycle("reader_a", LifecycleVerb.START); awaitRows(mongo, source, targetA, BOUND);
                control.lifecycle("reader_b", LifecycleVerb.START); awaitRows(mongo, source, targetB, BOUND);
                String beforeAck = Await.answered("the second consumer's initial target confirmation", BOUND,
                        () -> control.durablePosition("reader_b", "orders"));
                long beforeExecution = Await.answered("the second consumer's original execution", BOUND,
                        () -> control.executionGenerationOf("reader_b"));
                var cache = cacheClient.getDatabase(cacheDb); var generations = configClient.getDatabase(configDb).getCollection("operator_generations");
                var chains = cache.getCollection("srs_chains"); assertThat(chains.countDocuments()).isOne();
                var originalChain = chains.find().first();
                assertConsumer(cache, "reader_b", 1, originalChain.get("_id"));
                var models = cache.getCollection("srs_models").find().sort(new Document("_id", 1)).into(new ArrayList<>());
                assertThat(models).isNotEmpty();
                control.stop("reader_a", true);
                // Runtime reads remain fenced while CONFIG is publishing the replacement scope.
                Await.until("only the first consumer's explicit full replacement to complete", BOUND,
                        () -> {
                            var mapping = generations.find(new Document("_id", "reader_a")).first();
                            return mapping != null && Long.valueOf(2).equals(mapping.get("stateGeneration"))
                                    && "ACTIVE".equals(mapping.getString("phase"))
                                    && control.state("reader_a").filter(PipelineState.STOPPED::equals).isPresent();
                        }, () -> "mapping=" + generations.find(new Document("_id", "reader_a")).first());
                MongoDatabase admin = dataClient.getDatabase("admin");
                long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1)).append("data", new Document("failCommands", List.of("find"))
                                .append("namespace", sourceDb + ".orders").append("appName", appName)
                                .append("blockConnection", true).append("blockTimeMS", 15000))).get("count")).longValue();
                var fullCount = new Document("_id.scope.pipelineId", "reader_a").append("_id.scope.stateGeneration", 2L);
                try (var starter = Executors.newSingleThreadExecutor()) {
                    var start = starter.submit(() -> control.lifecycle("reader_a", LifecycleVerb.START));
                    try {
                        admin.runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", entered + 1).append("maxTimeMS", 15000));
                        Object held = Await.answered("the first consumer's real source read to be held", Duration.ofSeconds(5),
                                () -> heldRead(admin, appName, sourceDb)).get("opid");
                        assertThat(cache.getCollection("snapshot_counts").countDocuments(fullCount)).isZero();
                        mongo.update(source, "orders", Map.of("id", 2L), Map.of("seq", 222L));
                        awaitRows(mongo, source, targetB, Duration.ofSeconds(8));
                        Await.answered("the unaffected consumer's target confirmation during the held full start", Duration.ofSeconds(5),
                                () -> control.durablePosition("reader_b", "orders").filter(value -> !value.equals(beforeAck)));
                        assertThat(heldRead(admin, appName, sourceDb).map(op -> op.get("opid"))).contains(held);
                        assertThat(cache.getCollection("snapshot_counts").countDocuments(fullCount)).isZero();
                        assertConsumer(cache, "reader_b", 1, originalChain.get("_id"));
                        assertThat(control.state("reader_b")).contains(PipelineState.RUNNING);
                    } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
                    start.get(60, TimeUnit.SECONDS);
                }
                awaitRows(mongo, source, targetA, BOUND);
                Await.until("the replacement generation's actual full read to finish", BOUND,
                        () -> cache.getCollection("snapshot_counts").countDocuments(new Document(fullCount).append("rows", 8L)) == 1,
                        () -> "snapshot counts=" + cache.getCollection("snapshot_counts").find(fullCount).into(new ArrayList<>()));
                mongo.update(source, "orders", Map.of("id", 4L), Map.of("seq", 444L));
                awaitRows(mongo, source, targetA, BOUND); awaitRows(mongo, source, targetB, BOUND);
                assertThat(generations.find(new Document("_id", "reader_b")).first()).containsEntry("stateGeneration", 1L);
                assertThat(control.executionGenerationOf("reader_b")).contains(beforeExecution);
                assertConsumer(cache, "reader_a", 2, originalChain.get("_id"));
                assertConsumer(cache, "reader_b", 1, originalChain.get("_id"));
                assertThat(chains.countDocuments()).isOne();
                assertThat(chains.find().first()).containsEntry("_id", originalChain.get("_id")).containsEntry("epoch", originalChain.get("epoch"));
                assertThat(cache.getCollection("srs_models").find().sort(new Document("_id", 1)).into(new ArrayList<>())).isEqualTo(models);
                assertThat(control.state("reader_a")).contains(PipelineState.RUNNING);
                assertThat(control.state("reader_b")).contains(PipelineState.RUNNING);
            }
        }
    }

    private static Optional<Document> heldRead(MongoDatabase admin, String appName, String database) {
        return admin.runCommand(new Document("currentOp", 1).append("$all", true)).getList("inprog", Document.class).stream()
                .filter(op -> appName.equals(op.getString("appName")))
                .filter(op -> op.get("command") instanceof Document command && "orders".equals(command.getString("find"))
                        && database.equals(command.getString("$db"))).findFirst();
    }

    private static void assertConsumer(MongoDatabase cache, String pipeline, long generation, Object chain) {
        var consumers = cache.getCollection("srs_consumers").find(new Document("pipelineId", pipeline)
                .append("stateGeneration", generation).append("detached", false)).into(new ArrayList<>());
        assertThat(consumers).hasSize(1);
        assertThat(consumers.getFirst().get("_id", Document.class)).containsEntry("chain", chain);
    }

    private static void awaitRows(MongoEndpoints mongo, EndpointAddress source, EndpointAddress target, Duration bound) {
        Await.until("all actual target values to match the source", bound, () -> rows(mongo, source).equals(rows(mongo, target)),
                () -> "target=" + rows(mongo, target));
    }
    private static Map<Long, Long> rows(MongoEndpoints mongo, EndpointAddress endpoint) {
        var result = new TreeMap<Long, Long>();
        for (var row : mongo.documents(endpoint, "orders"))
            assertThat(result.put(((Number) row.get("id")).longValue(), ((Number) row.get("seq")).longValue())).isNull();
        return result;
    }
    private static String pipeline(String id, String target) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ { id: input, srs: true } ]
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: %s
                      on_full_load: clear
                """.formatted(id, target);
    }
    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
