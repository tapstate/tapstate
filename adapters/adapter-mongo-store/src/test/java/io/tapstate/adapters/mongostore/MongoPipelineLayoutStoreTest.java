package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineLayout;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoPipelineLayoutStoreTest {

    @Test
    void storesNodeIdsAsValuesSoReactFlowIdsCannotBecomeMongoFieldNames() {
        PipelineLayout layout = new PipelineLayout(
                "orders_sync",
                Map.of(
                        "source:mysql.orders", new PipelineLayout.NodePosition(80.5, 120),
                        "transform:clean-orders", new PipelineLayout.NodePosition(360, 120)),
                new PipelineLayout.Viewport(-12, 48, 0.8));

        Document stored = MongoPipelineLayoutStore.toDocument(layout);

        assertThat(stored).containsEntry("_id", "orders_sync");
        assertThat(stored.getList("nodes", Document.class)).containsExactlyInAnyOrder(
                new Document("id", "source:mysql.orders").append("x", 80.5).append("y", 120.0),
                new Document("id", "transform:clean-orders").append("x", 360.0).append("y", 120.0));
        assertThat(stored.get("nodes")).isNotInstanceOf(Document.class);
        assertThat(MongoPipelineLayoutStore.toLayout(stored)).isEqualTo(layout);
    }

    @Test
    void readsALayoutWithoutAViewport() {
        Document stored = new Document("_id", "orders_sync")
                .append("nodes", java.util.List.of(new Document("id", "source:orders").append("x", 0).append("y", 0)));

        assertThat(MongoPipelineLayoutStore.toLayout(stored)).isEqualTo(new PipelineLayout(
                "orders_sync",
                Map.of("source:orders", new PipelineLayout.NodePosition(0, 0)),
                null));
    }

    @Test
    void rejectsACorruptViewportWithTheStoreDiagnostic() {
        Document stored = new Document("_id", "orders_sync")
                .append("nodes", java.util.List.of())
                .append("viewport", new Document("x", 0).append("y", 0).append("zoom", 0));

        assertThatThrownBy(() -> MongoPipelineLayoutStore.toLayout(stored))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                    assertThat(error.args()).containsEntry("id", "orders_sync");
                });
    }
}
