package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.PipelineDraft;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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
