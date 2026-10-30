package io.tapstate.spi.store;

import java.util.Objects;

/** A structural model without row-count measurement; ddlSequence is its inclusive first log position. */
public record SrsModel(SrsModelKey key, String ringName, SourceTable table, long ddlSequence) {
    public SrsModel {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(table, "table");
        if (ringName == null || ringName.isBlank() || !key.table().equals(table.name()) || ddlSequence < 0) {
            throw new IllegalArgumentException("invalid SRS model");
        }
        table = new SourceTable(table.name(), table.fields(), table.primaryKey(), table.indexes());
    }
}
