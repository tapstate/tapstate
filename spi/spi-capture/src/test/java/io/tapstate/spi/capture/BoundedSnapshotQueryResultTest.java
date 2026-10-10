package io.tapstate.spi.capture;

import io.tapstate.core.event.Envelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedSnapshotQueryResultTest {

    @Test
    void normalizesNullRowsAndDefensivelyCopiesReturnedRows() {
        Instant sampledAt = Instant.now();
        assertThat(new BoundedSnapshotQueryResult(null, true, false, false, 0, sampledAt).rows()).isEmpty();
        List<Envelope> rows = new ArrayList<>(List.of(Envelope.read(1, "orders", Map.of("id", 1), Map.of())));
        BoundedSnapshotQueryResult result = new BoundedSnapshotQueryResult(rows, true, false, true, 1, sampledAt);
        rows.clear();
        assertThat(result.rows()).hasSize(1);
    }

    @Test
    void rejectsNegativeQueryCountsAndContradictoryCompletenessFlags() {
        Instant sampledAt = Instant.now();
        assertThatThrownBy(() -> new BoundedSnapshotQueryResult(List.of(), true, false, true, -1, sampledAt))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedSnapshotQueryResult(List.of(), true, true, true, 0, sampledAt))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
