package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A decimal a map expression only moves - {@code moved: "=after.amount"} - reaches its target, because the
 * moved column keeps the precision, scale and range its source column declared, and a target cannot build
 * a decimal column without them.
 *
 * <p>The harness connector reads every column as text, and the specification vocabulary has no word for a
 * declared decimal, so this case writes the descriptor a source's discovery gives a {@code DECIMAL(10,2)}
 * column into the stored model, between two server launches. Reverting the fix leaves the target empty and
 * the pipeline failed on a connector write failure that names the moved column.
 */
class ADecimalAMapOnlyMovesReachesItsTargetIT {

    private static final String SOURCE = "src_orders";
    private static final String PIPELINE = "moved_decimal";
    private static final String TABLE = "orders";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aDecimalMovedThroughAMapIsWrittenWithItsSourceDescriptor(@TempDir Path directory) throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Path jar = E2eConnectorJar.buildInto(directory);
        Files.writeString(source.resolve(TABLE + ".csv"), "id,amount\n7,70.00\n");
        Path written = target.resolve(TABLE + ".csv");
        String database = "e2e_moved_decimal_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(database);

        try (ServerHandle first = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(first.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));
            // Discovered before the batch, so the expression is judged against a known source.
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.apply(Map.of(
                    "src_orders.tap.yml", sourceYaml(source),
                    "tgt_orders.tap.yml", targetYaml(target),
                    "pipeline.tap.yml", pipelineYaml()));
        }

        // What a relational source's discovery says about a DECIMAL(10,2) column: the spelling, the type,
        // and the numeric descriptor a target builds the column from. Only that one stored field changes;
        // the server restarts against the same store.
        try (var client = MongoClients.create(storeUri)) {
            var schemas = client.getDatabase(database).getCollection("source_schemas");
            Document table = schemas.find(new Document("name", TABLE)).first();
            assertThat(table).isNotNull();
            List<Document> fields = table.getList("fields", Document.class);
            Document amount = fields.stream().filter(field -> "amount".equals(field.getString("name")))
                    .findFirst().orElseThrow();
            amount.put("type", "decimal(10,2)");
            amount.put("tapstateType", "DECIMAL");
            amount.put("numericType", new Document("bit", null).append("fixed", true)
                    .append("unsigned", null).append("zerofill", null)
                    .append("minValue", "-9999999999").append("maxValue", "9999999999")
                    .append("precision", 10).append("scale", 2));
            assertThat(schemas.replaceOne(new Document("_id", table.get("_id")), table).getMatchedCount())
                    .isEqualTo(1);
        }

        assertThat(written).doesNotExist();
        try (ServerHandle second = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(second.baseUrl());
            control.login("e2e", "e2e-password");
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the row carrying the moved decimal to reach the target",
                    () -> contents(written).lines().count() == 2,
                    () -> "target=" + contents(written) + ", state=" + control.state(PIPELINE)
                            + ", logs=" + control.logs(PIPELINE));
            List<String> lines = Files.readAllLines(written);
            List<String> header = Arrays.asList(lines.get(0).split(",", -1));
            List<String> row = Arrays.asList(lines.get(1).split(",", -1));
            assertThat(header).as("the target's columns").contains("moved");
            assertThat(row.get(header.indexOf("moved")))
                    .as("the moved decimal, as the target wrote it")
                    .isEqualTo("70.00");
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
                id: %s
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, directory, TABLE);
    }

    private static String targetYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_orders
                connector: e2e_file
                config: { uri: "%s" }
                """.formatted(directory);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_only }
                transforms:
                  - { id: moved_step, from: [%s], type: map, fields: { moved: "=after.amount" } }
                serve:
                  from: moved_step
                  sync:
                    - source: tgt_orders
                """.formatted(PIPELINE, SOURCE, TABLE);
    }
}
