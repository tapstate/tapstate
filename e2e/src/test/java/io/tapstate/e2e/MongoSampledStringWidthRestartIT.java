package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Ordinary MongoDB values can widen sampled metadata without changing the collection's structure. */
class MongoSampledStringWidthRestartIT {
    private static final String SOURCE = "width_source";
    private static final String PIPELINE = "width_restart";
    private static final String TABLE = "orders";

    @Test
    void longerSampledStringsDoNotBlockRestartWithTheRetainedModelAndCheckpoint() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
        try (var mongo = mongo()) {
            String sourceUri = mongo.getReplicaSetUrl("sampled_width_source");
            String targetUri = mongo.getReplicaSetUrl("sampled_width_target");
            try (var storage = PreparedNativeStore.createOn(mongo);
                    var sourceClient = MongoClients.create(sourceUri);
                    var targetClient = MongoClients.create(targetUri);
                    var cacheClient = MongoClients.create(storage.cacheUri())) {
                var source = sourceClient.getDatabase("sampled_width_source").getCollection(TABLE);
                var target = targetClient.getDatabase("sampled_width_target").getCollection(TABLE);
                var cache = cacheClient.getDatabase(storage.cacheDatabase());
                source.insertOne(new Document("_id", 1).append("name", "a"));
                try (var first = InProcessServer.startPrepared(storage)) {
                    var control = new ControlPlane(first.baseUrl());
                    storage.loginAndAwaitBusiness(control);
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    control.apply(workspace(sourceUri, targetUri));
                    control.discoverSchema(SOURCE, "mongodb", Map.of("uri", sourceUri, "database", "sampled_width_source"));
                    control.lifecycle(PIPELINE, LifecycleVerb.START);
                    Await.until("the short snapshot value to reach the target",
                            () -> target.countDocuments(new Document("_id", 1).append("name", "a")) == 1,
                            () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
                    Await.until("the initial snapshot to land",
                            () -> control.snapshotTables(PIPELINE).get(TABLE) != null
                                    && control.snapshotTables(PIPELINE).get(TABLE).landed(),
                            () -> "snapshot=" + control.snapshotTables(PIPELINE));
                    source.insertOne(new Document("_id", 2).append("name", "b"));
                    Await.until("a short CDC value to reach the target",
                            () -> target.countDocuments(new Document("_id", 2).append("name", "b")) == 1,
                            () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
                }

                var models = SystemCollections.SRS_MODELS.on(cache);
                List<Document> retained = models.find(new Document("_id.table", TABLE)).into(new ArrayList<>());
                assertThat(retained).as("the original model remains in storage after service shutdown").hasSize(1);
                Document savedSchema = retained.getFirst().get("schema", Document.class);
                Document chain = SystemCollections.SRS_CHAINS.on(cache).find().first();
                assertThat(chain).isNotNull();
                Document checkpoint = chain.get("checkpoint", Document.class);
                assertThat(checkpoint).as("restart retains the original capture checkpoint").isNotNull();
                assertThat(checkpoint.getList("tables", Document.class)).singleElement()
                        .satisfies(at -> assertThat(at.get("model")).isEqualTo(retained.getFirst().get("_id")));
                assertThat(checkpoint.getString("token")).isNotBlank();

                String longer = "x".repeat(4096);
                source.insertOne(new Document("_id", 3).append("name", longer));
                try (var second = InProcessServer.startPrepared(storage)) {
                    var control = new ControlPlane(second.baseUrl());
                    storage.loginAndAwaitBusiness(control);
                    control.discoverSchema(SOURCE, "mongodb", Map.of("uri", sourceUri, "database", "sampled_width_source"));
                    Document freshSchema = discoveredTable(cache);
                    Document savedName = field(savedSchema, "name");
                    Document freshName = field(freshSchema, "name");
                    long savedWidth = savedName.get("stringType", Document.class).getLong("bytes");
                    long freshWidth = freshName.get("stringType", Document.class).getLong("bytes");
                    assertThat(freshWidth).as("ordinary values increased the connector's sampled string width").isGreaterThan(savedWidth);
                    // The connector reports the sampled width in both its native spelling and string descriptor.
                    assertThat(savedName.getString("type")).isEqualTo("STRING(" + savedWidth + ")");
                    assertThat(freshName.getString("type")).isEqualTo("STRING(" + freshWidth + ")");
                    // Remove only those two width observations to compare every other structural attribute.
                    Document comparable = Document.parse(freshSchema.toJson());
                    field(comparable, "name").get("stringType", Document.class).put("bytes", savedWidth);
                    field(comparable, "name").put("type", savedName.getString("type"));
                    assertThat(comparable.getList("fields", Document.class)).containsExactlyInAnyOrderElementsOf(savedSchema.getList("fields", Document.class));
                    assertThat(comparable.get("primaryKey")).isEqualTo(savedSchema.get("primaryKey"));
                    assertThat(comparable.get("indexes")).isEqualTo(savedSchema.get("indexes"));
                    for (Document model : retained) {
                        assertThat(models.find(new Document("_id", model.get("_id"))).first())
                                .as("restart has not lost or replaced the retained model").isEqualTo(model);
                    }

                    Await.until("restart to deliver the longer value or report its refusal",
                            () -> target.countDocuments(new Document("_id", 3).append("name", longer)) == 1
                                    || control.state(PIPELINE).filter(PipelineState.FAILED::equals).isPresent(),
                            () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
                    assertThat(control.failure(PIPELINE))
                            .as("only sampled string width changed; saved=%s, fresh=%s, checkpoint=%s", savedName, freshName, checkpoint)
                            .isEmpty();
                    assertThat(target.countDocuments(new Document("_id", 3).append("name", longer)))
                            .as("the retained-state restart delivers the document inserted while stopped").isEqualTo(1);
                }
            }
        }
    }

    private static MongoDBContainer mongo() {
        var mongo = new MongoDBContainer(DockerImageName.parse(System.getProperty("tapstate.e2e.mongo-image", "mongo:7.0")))
                .withTmpFs(Map.of("/data/db", "rw,size=1g", "/data/configdb", "rw,size=16m"));
        String owner = System.getProperty("tapstate.e2e.resource-owner");
        if (owner != null && !owner.isBlank()) {
            mongo.withLabel(System.getProperty("tapstate.e2e.resource-owner-label", "io.tapstate.e2e.resource-owner"), owner);
        }
        mongo.start();
        return mongo;
    }

    private static Document discoveredTable(MongoDatabase cache) {
        var schemas = SystemCollections.CACHE_SOURCE_SCHEMAS.on(cache);
        Document envelope = schemas.find(new Document("_id", SOURCE)).first();
        assertThat(envelope).isNotNull();
        Document table = schemas.find(new Document("generation", envelope.get("generation"))
                .append("cacheScope", envelope.get("cacheScope")).append("name", TABLE)).first();
        assertThat(table).isNotNull();
        return table;
    }

    private static Document field(Document table, String name) {
        return table.getList("fields", Document.class).stream()
                .filter(field -> name.equals(field.getString("name"))).findFirst().orElseThrow();
    }

    private static Map<String, String> workspace(String sourceUri, String targetUri) {
        return Map.of("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: width_source
                connector: mongodb
                config: { uri: "%s", database: sampled_width_source }
                mode: cdc
                tables: [orders]
                """.formatted(sourceUri), "target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: width_target
                connector: mongodb
                config: { uri: "%s", database: sampled_width_target }
                """.formatted(targetUri), "pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: width_restart
                source: width_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: width_target
                """);
    }
}
