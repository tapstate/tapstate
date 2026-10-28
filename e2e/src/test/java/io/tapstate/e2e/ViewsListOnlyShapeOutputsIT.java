package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;

/** Real writes distinguish maintained collections from unrelated source tables and logical step ids. */
@RequiresDocker
class ViewsListOnlyShapeOutputsIT {
    @Test
    void unwindCatalogMatchesTheWrittenCollection() {
        verify("unwind", """
                  - id: shaped
                    type: unwind
                    from: orders
                    path: TAGS
                    include_array_index: TAG_NO
                    element_type: string
                """, 2);
    }

    @Test
    void nestCatalogMatchesTheWrittenRootCollection() {
        verify("nest", """
                  - id: shaped
                    type: nest
                    from: {o: orders, c: customers}
                    root:
                      from: o
                      key: [_id]
                      embed: [{from: c, on: {_id: customer_id}, as: object, path: customer}]
                """, 1);
    }

    @Test
    void joinCatalogMatchesTheWrittenFactCollection() {
        verify("join", """
                  - id: shaped
                    type: join
                    from: {o: orders, c: customers}
                    engine: builtin
                    sql: SELECT o._id AS _id, c.name AS customer_name FROM o LEFT JOIN c ON o.customer_id = c._id
                """, 1);
    }

    private static void verify(String shape, String transforms, long count) {
        RealConnectorGate.require("mongodb");
        var container = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db", "rw,size=512m", "/data/configdb", "rw,size=16m"))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withMemory(1L << 30).withNanoCPUs(1_000_000_000L));
        try (MongoDBContainer mongo = container) {
            mongo.start();
            String sourceUri = mongo.getReplicaSetUrl("source_db");
            String targetUri = mongo.getReplicaSetUrl("target_db");
            String collection = shape + "_orders";
            try (var client = MongoClients.create(sourceUri)) {
                var source = client.getDatabase("source_db");
                source.createCollection("orders");
                source.runCommand(new Document("collMod", "orders")
                        .append("changeStreamPreAndPostImages", new Document("enabled", true)));
                source.getCollection("orders").insertOne(new Document("_id", "order-1")
                        .append("customer_id", "customer-1").append("TAGS", List.of("red", "blue")));
                source.getCollection("customers").insertOne(new Document("_id", "customer-1").append("name", "Customer"));
                var target = client.getDatabase("target_db");
                try (ServerHandle server = Tiers.IN_PROCESS.launch(mongo.getReplicaSetUrl("control_db"), "operator_db")) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    control.bootstrapAndLogin("e2e", "e2e-password");
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    Map<String, String> resources = new LinkedHashMap<>();
                    resources.put("source.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: src
                            connector: mongodb
                            config: {uri: "%s", preImage: true}
                            mode: cdc
                            tables: [orders, customers]
                            """.formatted(sourceUri));
                    resources.put("target.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: target
                            connector: mongodb
                            config: {uri: "%s"}
                            """.formatted(targetUri));
                    control.apply(resources);
                    control.discoverSchema("src", "mongodb", Map.of("uri", sourceUri, "preImage", true));
                    resources.put("pipeline.tap.yml", "version: tapstate/v1\nkind: pipeline\nid: p\nsource: src\n"
                            + "settings: {read_mode: snapshot_and_cdc}\ntransforms:\n" + transforms + """
                            serve:
                              from: shaped
                              sync:
                                - id: output
                                  source: target
                                  write_mode: upsert
                                  rename: {prefix: "%s_"}
                            """.formatted(shape));
                    control.apply(resources);
                    control.lifecycle("p", LifecycleVerb.START);
                    Await.until("shape output reaches target", Duration.ofSeconds(120),
                            () -> target.getCollection(collection).countDocuments() == count, () -> control.logs("p"));
                    assertThat(target.listCollectionNames()).containsExactly(collection);
                    var response = control.sourceRequest("GET", "/api/views", null, null, 200);
                    Map<?, ?> body = (Map<?, ?>) JsonReader.parse(response.body());
                    assertThat((List<?>) body.get("unresolved")).isEmpty();
                    List<?> items = (List<?>) body.get("items");
                    assertThat(items).hasSize(1);
                    Map<?, ?> item = (Map<?, ?>) items.getFirst();
                    assertThat(item.get("location")).isEqualTo(Map.of(
                            "storeId", "target", "database", "target_db", "collection", collection));
                    assertThat(((Number) item.get("documents")).longValue()).isEqualTo(count);
                    var pipelineResponse = control.sourceRequest("GET", "/api/pipelines/p", null, null, 200);
                    Map<?, ?> pipeline = (Map<?, ?>) JsonReader.parse(pipelineResponse.body());
                    Map<?, ?> outputTargets = (Map<?, ?>) pipeline.get("outputTargets");
                    assertThat((List<?>) outputTargets.get("outputs")).singleElement()
                            .satisfies(value -> assertThat(((Map<?, ?>) value).get("table")).isEqualTo(collection));
                }
            }
        }
    }
}
