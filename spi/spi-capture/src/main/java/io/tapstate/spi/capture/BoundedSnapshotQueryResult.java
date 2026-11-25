package io.tapstate.spi.capture;

import io.tapstate.core.event.Envelope;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Result of a bounded source read. {@code hasMore} means matching rows were omitted by the row limit. */
public record BoundedSnapshotQueryResult(
        List<Envelope> rows,
        boolean complete,
        boolean hasMore,
        boolean repeatable,
        int queryCount,
        Instant sampledAt) {

    public BoundedSnapshotQueryResult {
        rows = rows == null ? List.of() : List.copyOf(rows);
        if (queryCount < 0) {
            throw new IllegalArgumentException("queryCount must not be negative");
        }
        if (complete == hasMore) {
            throw new IllegalArgumentException("complete must be the opposite of hasMore");
        }
        Objects.requireNonNull(sampledAt, "sampledAt");
    }
}
