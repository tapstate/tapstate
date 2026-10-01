package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest.AllRows;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest.ExactTuples;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest.Selection;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;
import io.tapstate.spi.capture.FieldSchema;
import io.tapstate.spi.capture.TableSchema;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PdkBoundedSnapshotQueryPortTest {

    @Test
    void exactCompositeTuplesAreQueriedSeparatelyAndNeverBecomeIndependentKeySets(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        Selection exact = new ExactTuples(List.of(
                Map.of("tenant_id", 1, "order_id", 1),
                Map.of("tenant_id", 2, "order_id", 2)));

        BoundedSnapshotQueryResult result = port.query(request(exact, 10));

        assertThat(result.complete()).isTrue();
        assertThat(result.hasMore()).isFalse();
        assertThat(result.queryCount()).isEqualTo(2);
        assertThat(result.rows()).extracting(row -> row.after().get("value"))
                .containsExactly("a", "b");
    }

    @Test
    void requestsOneExtraRowAndMarksAnAnchorResultIncompleteWhenTruncated(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));

        BoundedSnapshotQueryResult result = port.query(request(new AllRows(), 2));

        assertThat(result.rows()).hasSize(2);
        assertThat(result.complete()).isFalse();
        assertThat(result.hasMore()).isTrue();
        assertThat(result.repeatable()).isTrue();
    }

    @Test
    void refusesAConnectorWithoutExactQueryCapabilityBeforeTableRead(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.namedStreamSource(dir, "t1", "id")),
                "synthetic.NamedStreamSource", "2.0.8", null);
        PdkBoundedSnapshotQueryPort port = new PdkBoundedSnapshotQueryPort(connectorId -> ref);

        assertThatThrownBy(() -> port.query(request(new AllRows(), 1)))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.CAPABILITY_MISSING);
    }

    @Test
    void rejectsAnExpiredDeadlineWithoutOpeningTheConnector(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        BoundedSnapshotQueryRequest request = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), schema(), List.of("tenant_id", "order_id"),
                new AllRows(), List.of(), 1, Instant.EPOCH);

        assertThatThrownBy(() -> port.query(request))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.READ_TIMEOUT);
    }

    private static PdkBoundedSnapshotQueryPort port(Path jar) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.ExactTupleQuery", "2.0.8", null);
        return new PdkBoundedSnapshotQueryPort(connectorId -> ref);
    }

    private static BoundedSnapshotQueryRequest request(Selection selection, int limit) {
        return new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), schema(), List.of("tenant_id", "order_id"),
                selection, List.of(), limit, Instant.now().plusSeconds(5));
    }

    private static TableSchema schema() {
        return new TableSchema("t1", List.of(
                new FieldSchema("tenant_id", "int"),
                new FieldSchema("order_id", "int"),
                new FieldSchema("value", "string")));
    }
}
