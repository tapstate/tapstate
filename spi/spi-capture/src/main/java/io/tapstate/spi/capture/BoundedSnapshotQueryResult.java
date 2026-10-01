package io.tapstate.spi.capture;

import io.tapstate.core.event.Envelope;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Result of a bounded source read. Incomplete exact-tuple reads are never usable preview inputs. */
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
        Objects.requireNonNull(sampledAt, "sampledAt");
    }
}
