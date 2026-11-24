package io.tapstate.spi.store;

import java.util.Objects;

/**
 * Complete, ordered metadata observed at a source DDL boundary. This is a synchronous
 * capture receipt; the writer records the DDL under the preceding model and publishes
 * the next immutable model before admitting any following row. The receipt itself is
 * never written into the ring or durable row images.
 */
public record SourceSchemaChange(SourceTable before, SourceTable after) {
    public static final String ENVELOPE_KEY = "tapstate.capturedSchemaChange";

    public SourceSchemaChange {
        Objects.requireNonNull(before, "before");
        Objects.requireNonNull(after, "after");
        if (!before.name().equals(after.name())) throw new IllegalArgumentException("a column change keeps its table identity");
        before = new SourceTable(before.name(), before.fields(), before.primaryKey(), before.indexes());
        after = new SourceTable(after.name(), after.fields(), after.primaryKey(), after.indexes());
    }
}
