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

/** Bounded previews execute candidate transforms on both server tiers without changing the workspace. */
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
            Files.writeString(source.resolve("orders.csv"), "id,customer_id,company_name,amount\n"
                    + "order-1,customer-1,Northwind,12\n"
                    + "order-2,customer-2,Contoso,7\n"
                    + "order-3,customer-3,Fabrikam,9\n");
            Files.writeString(source.resolve("customers.csv"), "id,name\n"
                    + "customer-1,Northwind Ltd\n"
                    + "customer-2,Contoso Ltd\n"
                    + "customer-3,Fabrikam Ltd\n");
            Files.writeString(source.resolve("orders_archive.csv"), "id,customer_id,company_name,amount\n"
                    + "order-4,customer-3,Fabrikam,5\n");
            Files.writeString(source.resolve("order_items.csv"), "id,order_id,sku\n"
                    + "item-1,order-1,sku-a\n"
                    + "item-2,order-1,sku-b\n"
                    + "item-3,order-2,sku-c\n");
            Files.writeString(source.resolve("item_labels.csv"), "id,item_id,label\n"
                    + "label-1,item-1,fragile\n"
                    + "label-2,item-1,gift\n"
                    + "label-3,item-3,priority\n");
            Map<String, Object> settings = Map.of("uri", source.toString());
            control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, settings);

            List<ControlPlane.PreviewDraft> drafts = List.of(
                    new ControlPlane.PreviewDraft("preview_source.tap.yml", sourceYaml(source)),
                    new ControlPlane.PreviewDraft("preview_pipeline.tap.yml", pipelineYaml()));
            List<Map<String, Object>> events = control.preview(
                    PIPELINE_ID, OUTPUT_ID, 2, SAMPLE_ID, drafts);

            assertThat(events).extracting(event -> event.get("kind"))
                    .as("preview events including terminal failure payload: %s", events)
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

            Map<String, Object> filtered = result(control, source, filterPipelineYaml(), OUTPUT_ID,
                    SAMPLE_ID + "-filter", false);
            assertThat(filtered).containsEntry("complete", true);
            assertThat(((Number) filtered.get("rowCount")).intValue()).isEqualTo(1);
            assertThat(values(documents(filtered), "id"))
                    .containsExactly("order-1");

            Map<String, Object> allFiltered = result(control, source, emptyFilterPipelineYaml(), OUTPUT_ID,
                    SAMPLE_ID + "-filter-empty", false);
            assertThat(allFiltered).containsEntry("complete", true);
            assertThat(((Number) allFiltered.get("rowCount")).intValue()).isZero();
            assertThat(documents(allFiltered)).isEmpty();

            Map<String, Object> union = result(control, source, unionPipelineYaml(), OUTPUT_ID,
                    SAMPLE_ID + "-union", false);
            assertThat(union).containsEntry("complete", true);
            assertThat(((Number) union.get("rowCount")).intValue()).isEqualTo(4);
            assertThat(values(documents(union), "id"))
                    .containsExactlyInAnyOrder("order-1", "order-2", "order-3", "order-4");
            List<Map<String, Object>> boundedUnion = control.preview(PIPELINE_ID, OUTPUT_ID, 2,
                    SAMPLE_ID + "-union-bounded", List.of(
                            new ControlPlane.PreviewDraft("preview_source.tap.yml", sourceYaml(source)),
                            new ControlPlane.PreviewDraft("preview_pipeline.tap.yml", unionPipelineYaml())));
            Map<String, Object> unionSample = payload(boundedUnion, "sample.completed");
            assertThat(((Number) unionSample.get("rootRows")).intValue()).isEqualTo(2);
            assertThat(unionSample).containsEntry("rootTruncated", true);
            List<String> unionRootKeys = ((List<?>) unionSample.get("rootSourceKeys")).stream()
                    .map(String::valueOf)
                    .toList();
            assertThat(unionRootKeys).hasSize(2)
                    .containsExactlyElementsOf(unionRootKeys.stream().sorted().toList());

            Map<String, Object> joined = result(control, source, joinPipelineYaml(), OUTPUT_ID,
                    SAMPLE_ID + "-join", false);
            assertThat(joined).containsEntry("complete", true);
            assertThat(((Number) joined.get("rowCount")).intValue()).isEqualTo(3);
            Map<?, ?> joinedOrder = documentBy(documents(joined), "order_id", "order-1");
            assertThat(joinedOrder.get("customer_name")).isEqualTo("Northwind Ltd");

            Map<String, Object> nested = result(control, source, nestedPipelineYaml(), OUTPUT_ID,
                    SAMPLE_ID + "-nest", false);
            assertThat(nested).containsEntry("complete", true);
            assertThat(((Number) nested.get("rowCount")).intValue()).isEqualTo(3);
            Map<?, ?> firstOrder = documentBy(documents(nested), "id", "order-1");
            List<?> items = (List<?>) firstOrder.get("items");
            assertThat(items).hasSize(2);
            Map<?, ?> firstItem = documentBy(items, "id", "item-1");
            assertThat(values((List<?>) firstItem.get("labels"), "label"))
                    .containsExactlyInAnyOrder("fragile", "gift");
            Map<?, ?> secondItem = documentBy(items, "id", "item-2");
            assertThat((List<?>) secondItem.get("labels")).isEmpty();
            assertThat((List<?>) documentBy(documents(nested), "id", "order-3").get("items")).isEmpty();

            Map<String, Object> unwound = result(control, source, unwindPipelineYaml(), "preview_sync",
                    SAMPLE_ID + "-unwind", true);
            assertThat(unwound).containsEntry("format", "logical-json").containsEntry("complete", true);
            assertThat(((Number) unwound.get("rowCount")).intValue()).isEqualTo(6);
            assertThat(values(documents(unwound), "item_index").stream()
                    .map(Number.class::cast)
                    .map(Number::intValue)
                    .toList())
                    .containsExactly(0, 1, 0, 1, 0, 1);
            assertThat(values(documents(unwound), "items"))
                    .contains("first-item", "second-item");

            assertThat(control.artifactIds())
                    .as("preview compiles drafts without applying them to the artifact store")
                    .doesNotContain(SOURCE_ID, PIPELINE_ID);
            assertThat(Files.exists(source.resolve("preview-target")))
                    .as("preview replaces the target sink and never creates a target directory")
                    .isFalse();
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

    private static Map<String, Object> result(
            ControlPlane control, Path directory, String pipelineYaml, String outputId, String sampleId,
            boolean includeTarget) {
        List<ControlPlane.PreviewDraft> drafts = new java.util.ArrayList<>();
        drafts.add(new ControlPlane.PreviewDraft("preview_source.tap.yml", sourceYaml(directory)));
        drafts.add(new ControlPlane.PreviewDraft("preview_pipeline.tap.yml", pipelineYaml));
        if (includeTarget) {
            drafts.add(new ControlPlane.PreviewDraft(
                    "preview_target.tap.yml", targetYaml(directory.resolve("preview-target"))));
        }
        List<Map<String, Object>> events = control.preview(PIPELINE_ID, outputId, 100, sampleId, drafts);
        assertThat(events).extracting(event -> event.get("kind"))
                .startsWith("run.accepted", "compile.completed", "sample.completed")
                .endsWith("result.completed", "run.completed");
        return payload(events, "result.completed");
    }

    private static List<?> documents(Map<String, Object> result) {
        assertThat(result.get("documents")).isInstanceOf(List.class);
        return (List<?>) result.get("documents");
    }

    private static Map<?, ?> documentBy(List<?> documents, String key, Object value) {
        return documents.stream()
                .map(Map.class::cast)
                .filter(document -> value.equals(document.get(key)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no preview document matched " + key + "=" + value));
    }

    private static List<Object> values(List<?> documents, String key) {
        return documents.stream()
                .map(Map.class::cast)
                .map(document -> document.get(key))
                .toList();
    }

    private static String sourceYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: preview_source
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders, customers, orders_archive, order_items, item_labels ]
                """.formatted(directory);
    }

    private static String targetYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: preview_target
                connector: e2e_file
                config: { uri: "%s" }
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

    private static String filterPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: large_orders, from: [orders], type: filter, expr: "int(after.amount) >= 10" }
                view:
                  id: preview_view
                  from: large_orders
                  primary_key: id
                """;
    }

    private static String emptyFilterPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: no_orders, from: [orders], type: filter, expr: "false" }
                view:
                  id: preview_view
                  from: no_orders
                  primary_key: id
                """;
    }

    private static String unionPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_orders, from: [orders, orders_archive], type: union }
                view:
                  id: preview_view
                  from: all_orders
                  primary_key: id
                """;
    }

    private static String joinPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: joined_orders
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o.id AS order_id, o.amount AS amount, c.name AS customer_name
                      FROM o JOIN c ON o.customer_id = c.id
                view:
                  id: preview_view
                  from: joined_orders
                  primary_key: order_id
                """;
    }

    private static String nestedPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_tree
                    type: nest
                    from: { o: orders, i: order_items, l: item_labels }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - from: i
                          on: { order_id: id }
                          as: array
                          path: items
                          arrayKey: [ id ]
                          embed:
                            - { from: l, on: { item_id: id }, as: array, path: labels, arrayKey: [ id ] }
                view:
                  id: preview_view
                  from: order_tree
                  primary_key: id
                """;
    }

    private static String unwindPipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: preview_pipeline
                source: preview_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: decorated_orders
                    from: [orders]
                    type: map
                    fields:
                      items: ["first-item", "second-item"]
                  - { id: expanded_orders, from: [decorated_orders], type: unwind, path: items,
                      include_array_index: item_index }
                serve:
                  from: expanded_orders
                  sync:
                    - id: preview_sync
                      source: preview_target
                      write_mode: upsert
                """;
    }

    private static String storeUri(Tiers tier) {
        return SharedMongo.replicaSetUrl("bounded_preview_" + tier.name().toLowerCase(Locale.ROOT));
    }
}
