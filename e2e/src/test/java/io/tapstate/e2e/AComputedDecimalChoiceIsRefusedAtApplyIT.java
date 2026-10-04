package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.core.dsl.DslError;
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

/**
 * A decimal choice is refused through the real apply endpoint before any artifacts are stored.
 * The specification vocabulary cannot declare decimal metadata, so the harness source's discovered
 * text columns are replaced with complete decimal descriptors before the server reads them again.
 * The same choice applies and writes an integer when a preceding map replaces both decimal inputs.
 */
class AComputedDecimalChoiceIsRefusedAtApplyIT {

    private static final String SOURCE = "src_orders";
    private static final String TARGET = "tgt_orders";
    private static final String PIPELINE = "chosen_decimal";
    private static final String TABLE = "orders";
    private static final String EXPRESSION = "has(after.a) ? after.a : after.b";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aDecimalChoiceIsRefusedBeforeTheBatchIsStored(@TempDir Path directory) throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Path jar = E2eConnectorJar.buildInto(directory);
        Files.writeString(source.resolve(TABLE + ".csv"), "id,a,b\n7,70.00,80.00\n");
        String database = "e2e_decimal_choice_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(database);

        try (ServerHandle first = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(first.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
        }

        try (var client = MongoClients.create(storeUri)) {
            var schemas = client.getDatabase(database).getCollection("source_schemas");
            Document table = schemas.find(new Document("name", TABLE)).first();
            assertThat(table).isNotNull();
            List<Document> fields = table.getList("fields", Document.class);
            for (String name : List.of("a", "b")) {
                Document field = fields.stream().filter(value -> name.equals(value.getString("name")))
                        .findFirst().orElseThrow();
                field.put("type", "decimal(10,2)");
                field.put("tapstateType", "DECIMAL");
                field.put("numericType", new Document("bit", null).append("fixed", true)
                        .append("unsigned", null).append("zerofill", null)
                        .append("minValue", "-9999999999").append("maxValue", "9999999999")
                        .append("precision", 10).append("scale", 2));
            }
            assertThat(schemas.replaceOne(new Document("_id", table.get("_id")), table).getMatchedCount())
                    .isEqualTo(1);
        }

        try (ServerHandle second = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(second.baseUrl());
            control.login("e2e", "e2e-password");
            ControlPlane.Refusal refusal = control.applyExpectingRefusal(Map.of(
                    "src_orders.tap.yml", sourceYaml(source),
                    "tgt_orders.tap.yml", targetYaml(target),
                    "pipeline.tap.yml", pipelineYaml()));

            assertThat(refusal.code()).isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED.code());
            assertThat(refusal.params()).containsEntry("expr", EXPRESSION)
                    .containsEntry("column", "a").containsEntry("type", "DECIMAL")
                    .containsEntry("table", TABLE).containsEntry("path", "transforms[0].fields.chosen");
            assertThat(control.artifactIds()).doesNotContain(SOURCE, TARGET, PIPELINE);
            assertThat(target.resolve(TABLE + ".csv")).doesNotExist();

            control.apply(Map.of(
                    "src_orders.tap.yml", sourceYaml(source),
                    "tgt_orders.tap.yml", targetYaml(target),
                    "pipeline.tap.yml", pipelineYaml(true)));
            assertThat(control.artifactIds()).contains(SOURCE, TARGET, PIPELINE);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Path written = target.resolve(TABLE + ".csv");
            Await.until("the integer choice to reach the target", () -> contents(written).lines().count() == 2,
                    () -> "state=" + control.state(PIPELINE) + ", logs=" + control.logs(PIPELINE));
            List<String> lines = Files.readAllLines(written);
            List<String> header = Arrays.asList(lines.get(0).split(",", -1));
            List<String> row = Arrays.asList(lines.get(1).split(",", -1));
            assertThat(header).contains("chosen");
            assertThat(row.get(header.indexOf("chosen"))).isEqualTo("1");
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
                id: %s
                connector: e2e_file
                config: { uri: "%s" }
                """.formatted(TARGET, directory);
    }

    private static String pipelineYaml() {
        return pipelineYaml(false);
    }

    private static String pipelineYaml(boolean integerInputs) {
        String firstMap = integerInputs
                ? "  - { id: numbers, from: [orders], type: map, fields: { a: 1, b: 2 } }\n" : "";
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_only }
                transforms:
                %s\
                  - { id: chosen_step, from: [%s], type: map, fields: { chosen: "=%s" } }
                serve:
                  from: chosen_step
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE, firstMap, integerInputs ? "numbers" : TABLE, EXPRESSION, TARGET);
    }
}
