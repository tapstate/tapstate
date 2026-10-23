package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.util.Map;
import java.util.Objects;

/** A source batch position and every served table's captured durable frontier form one checkpoint. */
public record SrsCaptureCheckpoint(ChainPosition position, Map<String, SrsTableCheckpoint> tables) {
    public SrsCaptureCheckpoint {
        Objects.requireNonNull(position, "position");
        SourceOrder order = Objects.requireNonNull(position.order(), "order");
        Objects.requireNonNull(position.token(), "token");
        if (order.epoch() < 1 || order.epoch() == Long.MAX_VALUE || order.seq() == Long.MAX_VALUE
                || (order.seq() < 0 && order.seq() != SourceOrder.SNAPSHOT_SEQ)) {
            throw new IllegalArgumentException("invalid source checkpoint order");
        }
        tables = Map.copyOf(Objects.requireNonNull(tables, "tables"));
        if (tables.isEmpty()) throw new IllegalArgumentException("a capture checkpoint requires its served tables");
        for (var entry : tables.entrySet()) {
            if (entry.getKey().isBlank() || !entry.getKey().equals(entry.getValue().model().table())) {
                throw new IllegalArgumentException("a table checkpoint requires its exact model identity");
            }
        }
    }
}
