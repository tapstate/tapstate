package io.tapstate.spi.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BoundedSnapshotQueryRequestTest {

    @Test
    void exactTuplesKeepCompositeTupleBoundariesAndDefensivelyCopyTheirMaps() {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("tenant_id", 7);
        first.put("order_id", 11);
        BoundedSnapshotQueryRequest.ExactTuples selection =
                new BoundedSnapshotQueryRequest.ExactTuples(List.of(first));
        first.put("order_id", 99);

        assertThat(selection.tuples()).containsExactly(Map.of("tenant_id", 7, "order_id", 11));
    }

    @Test
    void exactTuplesRejectDifferentCompositeKeyShapes() {
        assertThatThrownBy(() -> new BoundedSnapshotQueryRequest.ExactTuples(List.of(
                Map.of("tenant_id", 7, "order_id", 11), Map.of("order_id", 12))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same key fields");
    }

    @Test
    void requestRejectsAnUnboundedOrOutOfTableSelection() {
        assertThatThrownBy(() -> new BoundedSnapshotQueryRequest(
                "source", "mysql", Map.of(), new TableSchema("orders", List.of(new FieldSchema("id", "int"))),
                List.of("missing"), new BoundedSnapshotQueryRequest.AllRows(), List.of(), 1,
                Instant.now().plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stableOrder");

        assertThatThrownBy(() -> new BoundedSnapshotQueryRequest(
                "source", "mysql", Map.of(), new TableSchema("orders", List.of()), List.of(),
                new BoundedSnapshotQueryRequest.AllRows(), List.of(), 20_001, Instant.now().plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit");
    }
}
