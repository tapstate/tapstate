package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Original connector bytes and an explicit full read reconstruct lost native models and operators. */
@RequiresDocker
class ThreeLayerCacheReconstructionIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    @Container private static final MongoDBContainer DATA = mongo();
    private static final Duration BOUND = Duration.ofSeconds(90);
    private static final List<String> PIPELINES = List.of("join_rebuild", "nest_rebuild");
    @TempDir Path work;

    @BeforeAll static void requireConnector() { RealConnectorGate.require("mongodb"); }

    @Test @Timeout(420)
    void originalJarDiscoveryAndConfirmedFullReconstructTheWholeNestAndJoinResultsAfterCacheLoss() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "reconstruct-" + suffix, configDb = "reconstruct_cfg_" + suffix;
        String cacheDb = "reconstruct_cache_" + suffix, sourceDb = "reconstruct_source_" + suffix;
        String targetDb = "reconstruct_target_" + suffix;
        String configUri = CONFIG.getReplicaSetUrl(configDb), sourceUri = DATA.getReplicaSetUrl(sourceDb);
        NativeMongoStorageFixture.prepare(configUri, new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb), cacheDb, null), DATA.getReplicaSetUrl(targetDb), cluster);
        byte[] originalJar = ConnectorJars.bytesFor("mongodb");
        String originalHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(originalJar));
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl());
                var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl());
                var dataClient = MongoClients.create(DATA.getReplicaSetUrl())) {
            var configuration = configClient.getDatabase(configDb);
            var cache = cacheClient.getDatabase(cacheDb);
            var source = dataClient.getDatabase(sourceDb); var target = dataClient.getDatabase(targetDb);
            source.getCollection("customers").insertMany(List.of(row(1).append("name", "Ada"), row(2).append("name", "Bo")));
            source.getCollection("orders").insertMany(List.of(row(100).append("customer_id", 1L).append("qty", 3L),
                    row(200).append("customer_id", 2L).append("qty", 4L)));
            source.getCollection("order_items").insertMany(List.of(row(11).append("order_id", 100L).append("sku", "first"),
                    row(12).append("order_id", 100L).append("sku", "second"), row(21).append("order_id", 200L).append("sku", "third")));
            var first = start(configUri, cluster);
            try (first) {
                var control = login(first); control.registerConnector("mongodb", originalJar);
                var resources = resources(sourceUri);
                resources.put("views.tap.yml", control.artifact("views").orElseThrow().canonicalForm());
                control.apply(resources);
                control.discoverSchema("input", "mongodb", Map.of("isUri", true, "uri", sourceUri));
                for (String pipeline : PIPELINES) { control.lifecycle(pipeline, LifecycleVerb.START); completed(control, pipeline); }
                awaitResults(source, target);
                assertThat(cache.getCollection("connector_artifacts.files").find().first()).isNotNull();
                assertThat(cache.getCollection("source_schemas").countDocuments()).isPositive();
                assertThat(cache.getCollection("derived_schemas").countDocuments()).isPositive();
                assertThat(cache.getCollection("nest_state").countDocuments()).isPositive();
                assertThat(cache.getCollection("join_facts").countDocuments()).isPositive();
                assertJarHash(cache, originalHash);
            }
            assertThat(first.isAlive()).as("all original members exit before this fixture's destructive fault").isFalse();
            var authority = configurationAuthority(configuration);
            // Only this isolated CACHE is lost. CONFIG and source/target data remain intact.
            cache.drop();
            assertThat(cache.listCollectionNames().into(new ArrayList<>())).isEmpty();
            source.getCollection("customers").updateOne(new Document("id", 1L), new Document("$set", new Document("name", "Ada updated")));
            source.getCollection("order_items").updateOne(new Document("id", 11L), new Document("$set", new Document("sku", "updated")));
            source.getCollection("order_items").insertOne(row(22).append("order_id", 200L).append("sku", "new"));
            target.getCollection("orders").insertOne(row(999).append("qty", -1L));
            target.getCollection("joined_orders").insertOne(row(999).append("order_id", 999L).append("customer_name", "stale"));
            try (var replacement = start(configUri, cluster)) {
                var control = login(replacement);
                assertThat(cache.getCollection("pipeline_state").countDocuments()).isZero();
                assertThat(cache.getCollection("connector_artifacts.files").countDocuments()).isZero();
                var plan = recovery(control, "plan", Map.of("scope", "CLUSTER", "pipelineIds", List.of()));
                assertThat(plan.get("affectedPipelineIds")).isEqualTo(PIPELINES);
                confirm(control, plan); recoveryComplete(control, plan);
                for (String pipeline : PIPELINES) {
                    assertThat(configuration.getCollection("pipeline_intents").find(new Document("_id", pipeline)).first())
                            .containsEntry("targetState", "STOPPED");
                    assertThat(control.state(pipeline)).isEmpty();
                }
                assertThat(cache.getCollection("nest_state").countDocuments()).isZero();
                assertThat(cache.getCollection("join_facts").countDocuments()).isZero();
                control.registerConnector("mongodb", originalJar); assertJarHash(cache, originalHash);
                control.discoverSchema("input", "mongodb", Map.of("isUri", true, "uri", sourceUri));
                String predecessor = (String) plan.get("operationId");
                for (String pipeline : PIPELINES) {
                    var full = recovery(control, "plan", Map.of("scope", "PIPELINE", "pipelineIds", List.of(pipeline),
                            "previousOperationId", predecessor));
                    assertThat(full.get("preflightCurrent")).isEqualTo(true);
                    var policies = (List<?>) full.get("targetPolicies"); assertThat(policies).hasSize(1);
                    assertThat(((Map<?, ?>) policies.getFirst()).get("policy")).isEqualTo("CLEAR");
                    confirm(control, full); recoveryComplete(control, full); completed(control, pipeline);
                    predecessor = (String) full.get("operationId");
                }
                awaitResults(source, target);
                assertThat(cache.getCollection("source_schemas").countDocuments()).isPositive();
                assertThat(cache.getCollection("derived_schemas").countDocuments()).isPositive();
                assertThat(cache.getCollection("nest_state").countDocuments()).isPositive();
                assertThat(cache.getCollection("join_facts").countDocuments()).isPositive();
                assertJarHash(cache, originalHash);
                assertThat(configurationAuthority(configuration)).isEqualTo(authority);
            }
        }
    }

    private RealProcessServer start(String configUri, String cluster) {
        return RealProcessServer.startPrepared(configUri, "127.0.0.1", port -> List.of(
                "--tapstate.cluster.id=" + cluster, "--tapstate.cluster.node-id=reconstruction-node",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds")));
    }
    private static ControlPlane login(RealProcessServer server) {
        var control = new ControlPlane(server.baseUrl()); control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
        return control;
    }
    private static void completed(ControlPlane control, String pipeline) {
        Await.until("the reconstructed bounded run to complete: " + pipeline, BOUND,
                () -> {
                    var state = control.state(pipeline);
                    if (state.filter(PipelineState.FAILED::equals).isPresent())
                        throw new AssertionError("bounded run failed: " + pipeline + " " + control.failure(pipeline));
                    return state.filter(PipelineState.COMPLETED::equals).isPresent();
                },
                () -> "state=" + control.state(pipeline) + ", failure=" + control.failure(pipeline) + ", logs=" + control.logs(pipeline));
    }
    private static Map<?, ?> recovery(ControlPlane control, String action, Map<String, ?> body) {
        return (Map<?, ?>) JsonReader.parse(control.sourceRequest("POST", "/api/storage/recovery/" + action, body, null, 200).body());
    }
    private static void confirm(ControlPlane control, Map<?, ?> plan) {
        recovery(control, "confirm", Map.of("operationId", plan.get("operationId"), "planHash", plan.get("planHash"),
                "controlRevision", plan.get("controlRevision"), "confirmed", true));
    }
    private static void recoveryComplete(ControlPlane control, Map<?, ?> plan) {
        Await.until("the original recovery operation to complete", BOUND,
                () -> "COMPLETE".equals(recovery(control, "status", Map.of("operationId", plan.get("operationId"))).get("phase")),
                () -> recovery(control, "status", Map.of("operationId", plan.get("operationId"))).toString());
    }
    private static void assertJarHash(MongoDatabase cache, String hash) {
        var files = cache.getCollection("connector_artifacts.files").find().into(new ArrayList<>());
        assertThat(files).hasSize(1);
        assertThat(files.getFirst().get("metadata", Document.class)).containsEntry("contentHash", hash);
        assertThat(cache.getCollection("connector_artifacts.chunks").countDocuments()).isPositive();
    }
    private static Document row(long id) { return new Document("_id", id).append("id", id); }
    private static Map<String, List<Document>> configurationAuthority(MongoDatabase configuration) {
        var result = new TreeMap<String, List<Document>>();
        for (String collection : List.of("storage_binding", "storage_setup", "op_bootstrap", "cluster_identity", "cluster_keys", "users")) {
            var records = configuration.getCollection(collection).find().sort(new Document("_id", 1)).into(new ArrayList<>());
            assertThat(records).isNotEmpty();
            if (collection.equals("cluster_keys")) for (var key : records) {
                assertThat(key.getString("keyId")).isNotBlank();
                assertThat(key.getString("material")).isNotBlank();
                assertThat(key.remove("sourceWriteFence")).isInstanceOf(Long.class);
            }
            result.put(collection, records);
        }
        return result;
    }
    private static List<Document> rows(MongoDatabase database, String table) {
        return database.getCollection(table).find().into(new ArrayList<>());
    }
    private static void awaitResults(MongoDatabase source, MongoDatabase target) {
        var expectedNest = new TreeMap<Long, Object>(); var expectedJoin = new TreeMap<Long, Object>();
        for (var order : rows(source, "orders")) {
            var document = new Document(order);
            document.put("items", rows(source, "order_items").stream().filter(item -> item.getLong("order_id").equals(order.getLong("id"))).toList());
            expectedNest.put(order.getLong("id"), normalized(document));
            var customer = source.getCollection("customers").find(new Document("id", order.getLong("customer_id"))).first();
            expectedJoin.put(order.getLong("id"), normalized(new Document("order_id", order.getLong("id"))
                    .append("customer_name", customer.getString("name")).append("qty", order.getLong("qty"))));
        }
        Await.until("all reconstructed Nest fields and joined values to match current source data", BOUND,
                () -> targetRows(target, "orders", "id").equals(expectedNest)
                        && targetRows(target, "joined_orders", "order_id").equals(expectedJoin),
                () -> "nest=" + targetRows(target, "orders", "id") + ", join=" + targetRows(target, "joined_orders", "order_id"));
    }
    private static Map<Long, Object> targetRows(MongoDatabase target, String table, String key) {
        var result = new TreeMap<Long, Object>();
        for (var row : rows(target, table)) {
            if (!(row.get(key) instanceof Number number)) throw new AssertionError("target row lacks its declared key: " + row);
            assertThat(result.put(number.longValue(), normalized(row))).isNull();
        }
        return result;
    }
    private static Object normalized(Object value) {
        if (value instanceof Map<?, ?> map) {
            var result = new TreeMap<String, Object>();
            map.forEach((key, item) -> { if (!"_id".equals(key)) result.put(String.valueOf(key), normalized(item)); });
            return result;
        }
        if (value instanceof List<?> list) return list.stream().map(ThreeLayerCacheReconstructionIT::normalized)
                .sorted(Comparator.comparing(Object::toString)).toList();
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)
            return ((Number) value).longValue();
        return value;
    }
    private static Map<String, String> resources(String sourceUri) {
        var result = new LinkedHashMap<String, String>();
        result.put("input.tap.yml", """
                version: tapstate/v1
                kind: source
                id: input
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                mode: snapshot
                tables: [ orders, order_items, customers ]
                """.formatted(sourceUri));
        result.put("nest_rebuild.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: nest_rebuild
                source: [ { id: input, srs: false } ]
                settings: { read_mode: snapshot_only }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - source: views
                      on_full_load: clear
                """);
        result.put("join_rebuild.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: join_rebuild
                source: [ { id: input, srs: false } ]
                settings: { read_mode: snapshot_only }
                transforms:
                  - id: widened
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o._id AS order_id, c.name AS customer_name, o.qty AS qty
                      FROM o JOIN c ON o.customer_id = c._id
                view:
                  id: joined_orders
                  from: widened
                  primary_key: order_id
                  on_full_load: clear
                """);
        return result;
    }
    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
