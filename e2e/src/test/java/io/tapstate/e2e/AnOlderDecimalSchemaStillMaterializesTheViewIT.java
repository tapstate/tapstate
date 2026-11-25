package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An upgraded server reads a persisted decimal spelling without numeric attributes and still writes
 * the view. The specification vocabulary cannot stage an older discovered model in the discovery store,
 * so this case changes that stored observation between two server launches. Reverting the decimal
 * bridge leaves the target empty and the pipeline reporting a connector write failure.
 */
class AnOlderDecimalSchemaStillMaterializesTheViewIT {

    private static final String SOURCE = "src_orders";
    private static final String PIPELINE = "old_decimal_view";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void aRowReachesTheViewAfterTheServerReadsAnOlderDecimalSchema(@TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Path jar = E2eConnectorJar.buildInto(directory);
        Files.writeString(source.resolve("orders.csv"), "id,amount\n7,70.00\n");
        Path view = target.resolve("orders.csv");
        try (PreparedNativeStore storage = PreparedNativeStore.create()) {
            try (ServerHandle first = InProcessServer.startPrepared(storage)) {
                ControlPlane control = new ControlPlane(first.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(Map.of(
                        "src_orders.tap.yml", sourceYaml(source),
                        "decimal_probe.tap.yml", targetYaml(target),
                        "pipeline.tap.yml", pipelineYaml()));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", source.toString()));
            }

            // The previous model kept the inferred type and exact source spelling but did not carry a
            // numeric descriptor. Change only that stored field; the server and its public resources restart
            // against the same durable store, while the source and target stay separate on disk.
            try (var client = MongoClients.create(storage.cacheUri())) {
                var schemas = SystemCollections.CACHE_SOURCE_SCHEMAS.on(client.getDatabase(storage.cacheDatabase()));
                Document published = schemas.find(new Document("_id", SOURCE)).first();
                assertThat(published).isNotNull();
                assertThat(published.get("cacheScope", Document.class).getString("clusterId")).isEqualTo(storage.clusterId());
                Document table = schemas.find(new Document("name", "orders")
                        .append("generation", published.get("generation"))
                        .append("cacheScope", published.get("cacheScope"))).first();
                assertThat(table).isNotNull();
                List<Document> fields = table.getList("fields", Document.class);
                Document amount = fields.stream().filter(field -> "amount".equals(field.getString("name")))
                        .findFirst().orElseThrow();
                amount.put("type", "decimal(10,2)");
                amount.put("tapstateType", "DECIMAL");
                amount.put("numericType", null);
                assertThat(schemas.replaceOne(new Document("_id", table.get("_id")), table).getMatchedCount())
                        .isEqualTo(1);
            }

            assertThat(view).doesNotExist();
            try (ServerHandle second = InProcessServer.startPrepared(storage)) {
                ControlPlane control = new ControlPlane(second.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the decimal row to reach the view", () -> contents(view).contains("70.00"),
                        () -> "view=" + contents(view) + ", state=" + control.state(PIPELINE)
                                + ", logs=" + control.logs(PIPELINE));
                assertThat(Files.readAllLines(view)).hasSize(2).last().asString().contains("70.00");
                try (var client = MongoClients.create(storage.masterUri())) {
                    var materialized = client.getDatabase(storage.masterDatabase()).getCollection("order_state");
                    Await.until("the same decimal row to reach the managed view", () -> materialized.countDocuments() == 1,
                            () -> "view rows=" + materialized.countDocuments() + ", logs=" + control.logs(PIPELINE));
                    assertThat(String.valueOf(materialized.find().first().get("amount"))).isEqualTo("70.00");
                }
            }
        }
    }

    private static String contents(Path file) {
        if (!Files.exists(file)) {
            return "";
        }
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sourceYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(directory);
    }

    private static String targetYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: decimal_probe
                connector: e2e_file
                config: { uri: "%s" }
                """.formatted(directory);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: old_decimal_view
                source: src_orders
                settings: { read_mode: snapshot_only }
                view:
                  id: order_state
                  from: orders
                  primary_key: id
                serve:
                  from: orders
                  sync: [{ source: decimal_probe }]
                """;
    }
}
