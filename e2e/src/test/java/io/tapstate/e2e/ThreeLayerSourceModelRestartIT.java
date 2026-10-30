package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Changed source discovery cannot reinterpret a previous process's recorded change-stream history. */
@RequiresDocker
class ThreeLayerSourceModelRestartIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    @Container private static final MongoDBContainer TARGET = mongo();
    @Container private static final MySQLContainer<?> SOURCE = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("model_history")
            .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    private static final String PIPELINE = "model_restart";
    private static final Duration BOUND = Duration.ofSeconds(90);
    @TempDir Path work;

    @BeforeAll static void requireConnectors() {
        RealConnectorGate.require("mysql", "mongodb");
        SharedMySql.grantReplication(SOURCE);
    }

    @Test @Timeout(300)
    void changedSourceSchemaRefusesRestartWithoutAdvancingTheOriginalModelOrPosition() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "model-" + suffix, cacheDb = "model_cache_" + suffix;
        String configUri = CONFIG.getReplicaSetUrl("model_cfg_" + suffix), targetUri = TARGET.getReplicaSetUrl("model_target_" + suffix);
        var mysql = Map.<String, Object>of("host", SOURCE.getHost(), "port", SOURCE.getMappedPort(3306),
                "database", SOURCE.getDatabaseName(), "username", SOURCE.getUsername(), "password", SOURCE.getPassword());
        sql(mysql, "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))",
                "INSERT INTO orders VALUES (1, 'seeded'), (2, 'unchanged')");
        NativeMongoStorageFixture.prepare(configUri, new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb), cacheDb, null), targetUri, cluster);
        var target = EndpointAddress.uri(targetUri);
        try (var mongo = new MongoEndpoints(); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var cache = cacheClient.getDatabase(cacheDb);
            var chains = SystemCollections.SRS_CHAINS.on(cache);
            var models = SystemCollections.SRS_MODELS.on(cache);
            String chainId, confirmed;
            var first = start(configUri, cluster);
            try (first) {
                var control = login(first);
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                var resources = new LinkedHashMap<String, String>();
                resources.put("views.tap.yml", control.artifact("views").orElseThrow().canonicalForm());
                resources.put("input.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: input
                        connector: mysql
                        config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                        mode: cdc
                        tables: [ orders ]
                        """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"), mysql.get("username"), mysql.get("password")));
                resources.put("model_restart.tap.yml", """
                        version: tapstate/v1
                        kind: pipeline
                        id: model_restart
                        source: [ { id: input, srs: true } ]
                        settings: { read_mode: snapshot_and_cdc }
                        serve:
                          from: orders
                          sync:
                            - source: views
                        """);
                control.apply(resources); control.discoverSchema("input", "mysql", mysql);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRows(mongo, target, Map.of(1L, "seeded", 2L, "unchanged"));
                String snapshotAck = Await.answered("the initial target confirmation", BOUND,
                        () -> control.durablePosition(PIPELINE, "orders"));
                sql(mysql, "UPDATE orders SET name='confirmed-cdc' WHERE id=1");
                awaitRows(mongo, target, Map.of(1L, "confirmed-cdc", 2L, "unchanged"));
                confirmed = Await.answered("the real CDC target confirmation", BOUND,
                        () -> control.durablePosition(PIPELINE, "orders").filter(value -> !value.equals(snapshotAck)));
                chainId = control.resumePoint(PIPELINE).get("chainId");
                assertThat(chainId).isNotBlank();
            }
            assertThat(first.isAlive()).isFalse();
            sql(mysql, "UPDATE orders SET name='same-model-downtime' WHERE id=1");
            var unchanged = start(configUri, cluster);
            try (unchanged) {
                var control = login(unchanged);
                awaitRows(mongo, target, Map.of(1L, "same-model-downtime", 2L, "unchanged"));
                Await.answered("the unchanged source model to resume its original history", BOUND,
                        () -> control.durablePosition(PIPELINE, "orders").filter(value -> !value.equals(confirmed)));
                assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry("orders", 0L);
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(control.resumePoint(PIPELINE)).containsEntry("chainId", chainId);
            }
            assertThat(unchanged.isAlive()).isFalse();
            var original = chains.find(new Document("_id", chainId)).first();
            assertThat(original).isNotNull();
            assertThat(original.get("checkpoint", Document.class)).isNotEmpty();
            assertThat(original.getLong("checkpointRevision")).isPositive();
            var originalModels = models.find().sort(new Document("_id", 1)).into(new ArrayList<>());
            assertThat(originalModels).isNotEmpty();
            assertThat(originalModels).allSatisfy(model -> assertThat(model.get("_id", Document.class)).containsEntry("modelVersion", 0L));
            var originalTarget = documents(mongo, target);
            sql(mysql, "ALTER TABLE orders ADD COLUMN added VARCHAR(64)",
                    "UPDATE orders SET name='pending-new-model', added='new-field' WHERE id=1",
                    "INSERT INTO orders VALUES (3, 'pending-insert', 'new-field')");
            assertThat(documents(mongo, target)).isEqualTo(originalTarget);
            try (var replacement = start(configUri, cluster)) {
                var control = login(replacement);
                var failure = Await.answered("an explicit refusal to reinterpret the old source history", BOUND,
                        () -> control.failure(PIPELINE));
                assertThat(failure.code()).isEqualTo(IoError.SRS_MODEL_UNAVAILABLE.code());
                assertThat(failure.params()).containsEntry("chain", chainId).containsEntry("table", "orders");
                assertThat(control.state(PIPELINE)).contains(PipelineState.FAILED);
            }
            var after = chains.find(new Document("_id", chainId)).first();
            assertThat(after).containsEntry("checkpoint", original.get("checkpoint"))
                    .containsEntry("checkpointRevision", original.get("checkpointRevision"));
            assertThat(models.find().sort(new Document("_id", 1)).into(new ArrayList<>())).isEqualTo(originalModels);
            assertThat(documents(mongo, target)).isEqualTo(originalTarget);
        }
    }

    private RealProcessServer start(String configUri, String cluster) {
        return RealProcessServer.startPrepared(configUri, "127.0.0.1", port -> List.of(
                "--tapstate.cluster.id=" + cluster, "--tapstate.cluster.node-id=model-node", "--tapstate.hz.member-port=0",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds")));
    }
    private static ControlPlane login(RealProcessServer server) {
        var control = new ControlPlane(server.baseUrl());
        control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
        return control;
    }
    private static void sql(Map<String, Object> settings, String... statements) throws Exception {
        try (var connection = SharedMySql.connect(settings); var statement = connection.createStatement()) {
            for (var sql : statements) statement.execute(sql);
        }
    }
    private static void awaitRows(MongoEndpoints mongo, EndpointAddress target, Map<Long, String> expected) {
        Await.until("the actual target values to match the source", BOUND, () -> rows(mongo, target).equals(expected),
                () -> "target=" + rows(mongo, target));
    }
    private static Map<Long, String> rows(MongoEndpoints mongo, EndpointAddress target) {
        var result = new TreeMap<Long, String>();
        for (var row : mongo.documents(target, "orders"))
            assertThat(result.put(((Number) row.get("id")).longValue(), String.valueOf(row.get("name")))).isNull();
        return result;
    }
    private static Map<Long, Document> documents(MongoEndpoints mongo, EndpointAddress target) {
        var result = new TreeMap<Long, Document>();
        for (var row : mongo.documents(target, "orders"))
            assertThat(result.put(((Number) row.get("id")).longValue(), row)).isNull();
        return result;
    }
    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
