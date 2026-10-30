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

    @Test
    void aMissingDiscoveryKeepsKnownSyncOutputsVisibleUntilItIsResolved() {
        verifyPartialDiscovery(false, "[orders]");
    }

    @Test
    void aMixedLiteralSelectorKeepsItsKnownOutputBeforeDiscovery() {
        verifyPartialDiscovery(true, "[orders, \"/customer.*/\"]");
    }

    @Test
    void aMixedStructuredSelectorKeepsItsKnownOutputBeforeDiscovery() {
        verifyPartialDiscovery(true, "[{name: orders}, \"/customer.*/\"]");
    }

    private static void verifyPartialDiscovery(boolean sameSource, String selectors) {
        RealConnectorGate.require("mongodb");
        try (MongoDBContainer mongo = mongoFixture()) {
            mongo.start();
            String sourceUri = mongo.getReplicaSetUrl("source_db");
            try (var client = MongoClients.create(sourceUri)) {
                client.getDatabase("source_db").getCollection("orders")
                        .insertOne(new Document("_id", "order-1"));
                client.getDatabase("source_db").getCollection("customers")
                        .insertOne(new Document("_id", "customer-1"));
                client.getDatabase("target_db").getCollection("out_orders")
                        .insertOne(new Document("_id", "order-1"));
                try (var storage = PreparedNativeStore.createOn(mongo);
                        ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    storage.loginAndAwaitBusiness(control);
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    Map<String, String> resources = new LinkedHashMap<>();
                    resources.put("sources.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: src
                            connector: mongodb
                            config: {uri: "%s"}
                            mode: cdc
                            tables: %s
                            """.formatted(sourceUri, selectors));
                    if (!sameSource) resources.put("unknown.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: unknown
                            connector: mongodb
                            config: {uri: "%s"}
                            mode: cdc
                            tables: ["/customer.*/"]
                            """.formatted(sourceUri));
                    resources.put("target.tap.yml", """
                            version: tapstate/v1
                            kind: source
                            id: target
                            connector: mongodb
                            config: {uri: "%s"}
                            """.formatted(mongo.getReplicaSetUrl("target_db")));
                    resources.put("pipeline.tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: p
                            source: %s
                            serve:
                              from: [src.orders, %s]
                              sync: [{id: output, source: target, rename: {prefix: out_}}]
                            """.formatted(sameSource ? "src" : "[src, unknown]", sameSource ? "src" : "unknown"));
                    control.apply(resources);
                    Map<?, ?> before = (Map<?, ?>) JsonReader.parse(
                            control.sourceRequest("GET", "/api/views", null, null, 200).body());
                    assertThat((List<?>) before.get("items")).singleElement().satisfies(value -> {
                        Map<?, ?> row = (Map<?, ?>) value;
                        assertThat(row.get("location")).isEqualTo(Map.of(
                                "storeId", "target", "database", "target_db", "collection", "out_orders"));
                        assertThat(((Number) row.get("documents")).longValue()).isEqualTo(1);
                    });
                    assertThat((List<?>) before.get("unresolved")).singleElement().satisfies(value ->
                            assertThat(((Map<?, ?>) value).get("code")).isEqualTo("actuation.source-schema-not-discovered"));
                    control.discoverSchema(sameSource ? "src" : "unknown", "mongodb", Map.of("uri", sourceUri));
                    Map<?, ?> after = (Map<?, ?>) JsonReader.parse(
                            control.sourceRequest("GET", "/api/views", null, null, 200).body());
                    assertThat((List<?>) after.get("unresolved")).isEmpty();
                    List<?> rows = (List<?>) after.get("items");
                    assertThat(rows).extracting(value -> (String) ((Map<?, ?>) ((Map<?, ?>) value).get("location")).get("collection"))
                            .containsExactlyInAnyOrder("out_orders", "out_customers");
                    assertThat(client.getDatabase("target_db").listCollectionNames()).containsExactly("out_orders");
                }
            }
        }
    }

    private static MongoDBContainer mongoFixture() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db", "rw,size=512m", "/data/configdb", "rw,size=16m"))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withMemory(1L << 30).withNanoCPUs(1_000_000_000L));
    }

    private static void verify(String shape, String transforms, long count) {
        RealConnectorGate.require("mongodb");
        var container = mongoFixture();
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
                try (var storage = PreparedNativeStore.createOn(mongo);
                        ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    storage.loginAndAwaitBusiness(control);
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
                    var observations = client.getDatabase(storage.cacheDatabase()).getCollection("pipeline_observation");
                    Await.until("current-run counts reach the native cache projection", Duration.ofSeconds(30), () -> {
                        Document observed = observations.find(new Document("_id", "p")).first();
                        if (observed == null || !(observed.get("initialLoad") instanceof Document load)
                                || !(load.get("tables") instanceof Document tables)
                                || !(tables.get("orders") instanceof Document orders)) return false;
                        return Long.valueOf(1).equals(orders.get("rowsRead"));
                    }, () -> control.logs("p"));
                    var storedObservation = observations.find(new Document("_id", "p")).first();
                    assertThat(storedObservation.get("scope", Document.class).getString("pipelineId")).isEqualTo("p");
                    Map<?, ?> snapshot = (Map<?, ?>) JsonReader.parse(control.sourceRequest(
                            "GET", "/api/pipelines/p/snapshot", null, null, 200).body());
                    Map<?, ?> load = (Map<?, ?>) snapshot.get("initialLoad");
                    assertThat(load).isNotNull();
                    assertThat(load.get("runKey")).isEqualTo(storedObservation.get("initialLoad", Document.class).getString("runKey"));
                    assertThat(load.get("readMode")).isEqualTo("snapshot_and_cdc");
                    assertThat(((Number) ((Map<?, ?>) ((Map<?, ?>) load.get("tables")).get("orders")).get("rowsRead")).longValue()).isEqualTo(1);
                    assertThat(client.getDatabase(storage.configurationDatabase()).listCollectionNames()).doesNotContain("pipeline_observation");
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
                    Map<String, Object> echo = new LinkedHashMap<>();
                    pipeline.forEach((key, value) -> echo.put((String) key, value));
                    echo.put("outputTargets", Map.of("outputs", List.of(Map.of(
                            "element", "output", "kind", "view", "connection", "target",
                            "table", "forged_collection", "identity", "forged")), "unresolved", List.of()));
                    var updated = control.sourceRequest("PUT", "/api/pipelines/p", echo,
                            pipelineResponse.headers().firstValue("ETag").orElseThrow(), 200);
                    Map<?, ?> updatedPipeline = (Map<?, ?>) JsonReader.parse(updated.body());
                    assertThat(updatedPipeline.get("outputTargets")).isEqualTo(outputTargets);
                    assertThat(updatedPipeline.get("contentHash")).isEqualTo(pipeline.get("contentHash"));
                }
            }
        }
    }
}
