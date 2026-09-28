package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An upgraded server reads a persisted decimal spelling without numeric attributes and still writes
 * the view. The specification vocabulary cannot stage an older discovered model in the control store,
 * so this case changes that stored observation between two server launches. Reverting the decimal
 * bridge leaves the target empty and the pipeline reporting a connector write failure.
 */
class AnOlderDecimalSchemaStillMaterializesTheViewIT {

    private static final String SOURCE = "src_orders";
    private static final String PIPELINE = "old_decimal_view";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRowReachesTheViewAfterTheServerReadsAnOlderDecimalSchema(@TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Path jar = E2eConnectorJar.buildInto(directory);
        Files.writeString(source.resolve("orders.csv"), "id,amount\n7,70.00\n");
        Path view = target.resolve("order_state.csv");
        String database = "e2e_old_decimal_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(database);

        try (ServerHandle first = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(first.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));
            control.apply(Map.of(
                    "src_orders.tap.yml", sourceYaml(source),
                    "views.tap.yml", viewYaml(target),
                    "pipeline.tap.yml", pipelineYaml()));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", source.toString()));
        }

        // The previous model kept the inferred type and exact source spelling but did not carry a
        // numeric descriptor. Change only that stored field; the server and its public resources restart
        // against the same durable store, while the source and target stay separate on disk.
        try (var client = MongoClients.create(storeUri)) {
            var schemas = client.getDatabase(database).getCollection("source_schemas");
            Document table = schemas.find(new Document("name", "orders")).first();
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
        try (ServerHandle second = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(second.baseUrl());
            control.login("e2e", "e2e-password");
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the decimal row to reach the view", () -> contents(view).contains("70.00"),
                    () -> "view=" + contents(view) + ", state=" + control.state(PIPELINE)
                            + ", logs=" + control.logs(PIPELINE));
            assertThat(Files.readAllLines(view)).hasSize(2).last().asString().contains("70.00");
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

    private static String viewYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: views
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
                """;
    }
}
