package io.tapstate.spi.store;

import java.util.Objects;

/** Compares source observations, never persistent record integrity or actual row admissibility. */
public enum SourceModelComparison {
    DECLARED_STRUCTURE,
    SAMPLED_DOCUMENT;

    /** The host must prove the source implementation and its retained origin before selecting sampling. */
    public boolean acceptsObservation(SourceTable retained, SourceTable observed) {
        Objects.requireNonNull(retained, "retained");
        Objects.requireNonNull(observed, "observed");
        if (!retained.name().equals(observed.name())) return false;
        // Sample membership, order and types describe documents observed, not a collection-wide schema.
        // Actual values still pass the ordinary expression, conversion, key and connector-write checks.
        if (this == SAMPLED_DOCUMENT) return true;
        return structural(retained).equals(structural(observed));
    }

    private static SourceTable structural(SourceTable table) {
        return new SourceTable(table.name(), table.fields(), table.primaryKey(), table.indexes());
    }
}
