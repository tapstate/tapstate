package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.PipelineDraft;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MongoPipelineDraftStoreTest {

    @Test
    void roundTripsTheVersionedDraftShapeWithoutCollapsingModePayloads() {
        PipelineDraft draft = new PipelineDraft("orders", 1, 3, PipelineDraft.Mode.DAG,
                "Orders", "draft", new PipelineDraft.Graph(
                        List.of(new PipelineDraft.Node("source", "source", "crm", "orders",
                                Map.of("selected", true), Map.of("label", "Orders"))),
                        List.of(), new PipelineDraft.Viewport(1, 2, 0.75)), null,
                "hash-1", 2L, "hash-1", Instant.parse("2026-09-21T00:00:00Z"),
                Instant.parse("2026-09-21T01:00:00Z"), "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored).isEqualTo(draft);
        assertThat(MongoPipelineDraftStore.toDocument(draft)).containsEntry("mode", "dag");
        assertThat(MongoPipelineDraftStore.toDocument(draft)).containsKey("graph").doesNotContainKey("wizard");
    }

    @Test
    void roundTripsDraftNodeConfigWithUnconfiguredNullValues() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connectorId", null);
        PipelineDraft draft = new PipelineDraft("incomplete", 1, 1, PipelineDraft.Mode.DAG,
                "Incomplete", "", new PipelineDraft.Graph(
                        List.of(new PipelineDraft.Node("target", "target", null, null, config, Map.of())),
                        List.of(), new PipelineDraft.Viewport(0, 0, 1)), null,
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored.graph().nodes().getFirst().config()).containsKey("connectorId");
        assertThat(restored.graph().nodes().getFirst().config().get("connectorId")).isNull();
    }

    @Test
    void roundTripsWizardRelationsTransformsAndNullableOutput() {
        PipelineDraft draft = new PipelineDraft("orders", 1, 4, PipelineDraft.Mode.WIZARD,
                "Orders", "assembled", null,
                new PipelineDraft.Wizard(
                        new PipelineDraft.Root("orders", "mysql", "orders", List.of("id"),
                                List.of(new PipelineDraft.Transform("normalize-root", "map",
                                        Map.of("customer", "$customer_name", "discard", false, "active", true)))),
                        List.of(new PipelineDraft.Related("items", "orders", "mysql", "order_items",
                                new PipelineDraft.Relation(
                                        List.of(new PipelineDraft.FieldPair("order_id", "id"),
                                                new PipelineDraft.FieldPair("tenant_id", "tenant_id")),
                                        PipelineDraft.Shape.ARRAY, "items", List.of("item_id"), List.of("item_id")),
                                List.of(new PipelineDraft.Transform("keep-items", "filter",
                                        Map.of("expr", "active == true"))))),
                        List.of(new PipelineDraft.Transform("post-map", "map", Map.of("count", 1))), null),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored).isEqualTo(draft);
        assertThat(restored.wizard().output()).isNull();
        assertThat(restored.wizard().related().getFirst().relation().on())
                .containsExactly(new PipelineDraft.FieldPair("order_id", "id"),
                        new PipelineDraft.FieldPair("tenant_id", "tenant_id"));
        assertThat(restored.wizard().root().preTransforms().getFirst().fields())
                .containsEntry("customer", "$customer_name").containsEntry("discard", false);
    }

    @Test
    void readsDocumentMapsAndMissingLegacyFieldsWithoutLosingDefaults() {
        Document legacy = new Document("_id", "legacy-wizard")
                .append("mode", "wizard")
                .append("revision", 2L)
                .append("name", "Legacy")
                .append("updatedBy", "migration")
                .append("wizard", new Document("root", new Document("id", "root")
                        .append("sourceId", "mysql").append("table", "orders")
                        .append("key", "not-a-list")
                        .append("preTransforms", List.of(Map.of("id", "rename", "type", "map",
                                "fields", Map.of("customer", "$name")))))
                        .append("related", List.of(Map.of(
                                "id", "items", "parentId", "root", "sourceId", "mysql", "table", "order_items",
                                "relation", new Document("on", List.of(Map.of("childField", "order_id", "parentField", "id")))
                                        .append("shape", "array").append("path", "items")
                                        .append("key", "not-a-list").append("arrayKey", List.of("item_id")),
                                "preTransforms", "not-a-list")))
                        .append("transforms", "not-a-list")
                        .append("output", null));

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(legacy);

        assertThat(restored.schemaVersion()).isEqualTo(PipelineDraft.CURRENT_SCHEMA_VERSION);
        assertThat(restored.createdAt()).isEqualTo(Instant.EPOCH);
        assertThat(restored.updatedAt()).isEqualTo(Instant.EPOCH);
        assertThat(restored.wizard().root().key()).isEmpty();
        assertThat(restored.wizard().root().preTransforms()).hasSize(1);
        assertThat(restored.wizard().related().getFirst().relation().key()).isEmpty();
        assertThat(restored.wizard().related().getFirst().preTransforms()).isEmpty();
        assertThat(restored.wizard().transforms()).isEmpty();
        assertThat(restored.wizard().output()).isNull();
    }

    @Test
    void migratesTheOriginalUnversionedEmptyDraftToSchemaOne() {
        Document legacy = new Document("_id", "orders")
                .append("revision", 1L)
                .append("name", "Orders")
                .append("description", "")
                .append("createdAt", java.util.Date.from(Instant.parse("2026-09-21T00:00:00Z")))
                .append("updatedAt", java.util.Date.from(Instant.parse("2026-09-21T00:00:00Z")))
                .append("updatedBy", "migration");

        Document migrated = PipelineDraftMigrations.migrate(legacy);
        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(migrated);

        assertThat(migrated.getInteger("schemaVersion")).isEqualTo(1);
        assertThat(restored.mode()).isEqualTo(PipelineDraft.Mode.DAG);
        assertThat(restored.graph().nodes()).isEmpty();
        assertThat(restored.graph().viewport().zoom()).isEqualTo(1D);
    }
}
