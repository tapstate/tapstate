package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The bounded preview reads a candidate, executes its Map step, and leaves the workspace untouched. */
class BoundedPipelinePreviewIT {

    private static final String SOURCE_ID = "preview_source";
    private static final String PIPELINE_ID = "preview_pipeline";
    private static final String OUTPUT_ID = "preview_view";
    private static final String SAMPLE_ID = "preview-e2e-sample";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void runsTheUnsavedCandidateWithABoundedSourceSampleOnBothTiers(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (ServerHandle server = tier.launch(storeUri(tier))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));

            Path source = Files.createDirectories(directory.resolve("source"));
            Files.writeString(source.resolve("orders.csv"), "id,company_name,amount\n"
                    + "order-1,Northwind,12\n"
                    + "order-2,Contoso,7\n"
                    + "order-3,Fabrikam,9\n");
            Map<String, Object> settings = Map.of("uri", source.toString());
            control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, settings);

            List<ControlPlane.PreviewDraft> drafts = List.of(
                    new ControlPlane.PreviewDraft("preview_source.tap.yml", sourceYaml(source)),
                    new ControlPlane.PreviewDraft("preview_pipeline.tap.yml", pipelineYaml()));
            List<Map<String, Object>> events = control.preview(
                    PIPELINE_ID, OUTPUT_ID, 2, SAMPLE_ID, drafts);

            assertThat(events).extracting(event -> event.get("kind"))
                    .startsWith("run.accepted", "compile.completed", "sample.completed")
                    .endsWith("result.completed", "run.completed");
            List<Long> sequences = events.stream()
                    .map(event -> ((Number) event.get("seq")).longValue())
                    .toList();
            assertThat(sequences)
                    .containsExactlyElementsOf(java.util.stream.LongStream.range(0, events.size()).boxed().toList());
            assertThat(events).extracting(event -> event.get("runId"))
                    .containsOnly(events.getFirst().get("runId"));
            assertThat(events).extracting(event -> event.get("candidateHash"))
                    .containsOnly(events.getFirst().get("candidateHash"));

            Map<String, Object> sample = payload(events, "sample.completed");
            assertThat(sample).containsEntry("complete", true).containsEntry("rootTruncated", true);
            assertThat(((Number) sample.get("rootRows")).intValue()).isEqualTo(2);
            assertThat(((Number) sample.get("queryCount")).intValue()).isEqualTo(1);

            Map<String, Object> result = payload(events, "result.completed");
            assertThat(result).containsEntry("format", "logical-json").containsEntry("complete", true);
            assertThat(((Number) result.get("rowCount")).intValue()).isEqualTo(2);
            assertThat(result.get("documents")).isInstanceOf(List.class);
            List<?> documents = (List<?>) result.get("documents");
            assertThat(documents).hasSize(2);
            assertDocument(documents.get(0), "order-1", "Northwind", 24);
            assertDocument(documents.get(1), "order-2", "Contoso", 14);

            List<Map<String, Object>> cached = control.preview(PIPELINE_ID, OUTPUT_ID, 2, SAMPLE_ID, drafts);
            assertThat(payload(cached, "sample.completed")).containsEntry("cacheHit", true);
            assertThat(payload(cached, "result.completed").get("documents")).isEqualTo(documents);

            List<ControlPlane.PreviewDraft> javascriptDrafts = List.of(
                    new ControlPlane.PreviewDraft("preview_source.tap.yml", sourceYaml(source)),
                    new ControlPlane.PreviewDraft("preview_pipeline.tap.yml", javascriptPipelineYaml()));
            List<Map<String, Object>> javascriptEvents = control.preview(
                    PIPELINE_ID, OUTPUT_ID, 2, SAMPLE_ID + "-javascript", javascriptDrafts);
            Map<String, Object> javascriptResult = payload(javascriptEvents, "result.completed");
            assertThat(javascriptResult).containsEntry("format", "logical-json");
            assertThat(javascriptResult.get("documents")).isInstanceOf(List.class);
            List<?> javascriptDocuments = (List<?>) javascriptResult.get("documents");
            assertThat(javascriptDocuments).hasSize(2);
            assertDocument(javascriptDocuments.get(0), "order-1", "Northwind", 24);
            assertDocument(javascriptDocuments.get(1), "order-2", "Contoso", 14);

            assertThat(control.artifactIds())
                    .as("preview compiles drafts without applying them to the artifact store")
                    .doesNotContain(SOURCE_ID, PIPELINE_ID);
        }
    }

    private static void assertDocument(Object value, String id, String company, int doubledAmount) {
        assertThat(value).isInstanceOf(Map.class);
        Map<?, ?> document = (Map<?, ?>) value;
        assertThat(document.get("id")).isEqualTo(id);
        assertThat(document.get("company")).isEqualTo(company);
        assertThat(document.containsKey("amount")).isFalse();
        assertThat(document.get("doubled_amount")).isInstanceOf(Number.class);
        assertThat(((Number) document.get("doubled_amount")).intValue()).isEqualTo(doubledAmount);
    }

    private static Map<String, Object> payload(List<Map<String, Object>> events, String kind) {
        Map<String, Object> event = events.stream()
                .filter(candidate -> kind.equals(candidate.get("kind")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("preview emitted no " + kind + " event: " + events));
        if (!(event.get("payload") instanceof Map<?, ?> payload)) {
            throw new AssertionError(kind + " carried no payload object: " + event);
        }
        return payload.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                entry -> String.valueOf(entry.getKey()), Map.Entry::getValue));
    }

    private static String sourceYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: preview_source
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(directory);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: projected_orders
                    from: [ orders ]
                    type: map
                    fields:
                      id: $id
                      company: $company_name
                      doubled_amount: "=int(after.amount) * 2"
                      amount: false
                view:
                  id: preview_view
                  from: projected_orders
                  primary_key: id
                """;
    }

    private static String javascriptPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: scripted_orders
                    from: [ orders ]
                    type: js
                    script: |
                      function process(record, ctx) {
                        record.after.company = record.after.company_name;
                        record.after.doubled_amount = Number(record.after.amount) * 2;
                        delete record.after.company_name;
                        delete record.after.amount;
                        return record;
                      }
                view:
                  id: preview_view
                  from: scripted_orders
                  primary_key: id
                """;
    }

    private static String storeUri(Tiers tier) {
        return SharedMongo.replicaSetUrl("bounded_preview_" + tier.name().toLowerCase(Locale.ROOT));
    }
}
