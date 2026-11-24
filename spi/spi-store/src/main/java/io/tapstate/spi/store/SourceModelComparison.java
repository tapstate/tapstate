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
        var fields = table.fields().stream().map(field -> new SourceField(field.name(), sqlTypeSpelling(field.dataType()),
                field.type(), field.unknownBecause(), field.numericType(), field.stringType())).toList();
        return new SourceTable(table.name(), fields, table.primaryKey(), table.indexes());
    }
    /** Built-in SQL keywords are case insensitive; saved spellings and quoted/literal types stay exact. */
    private static String sqlTypeSpelling(String nativeType) {
        if (nativeType == null || nativeType.indexOf('\"') >= 0 || nativeType.indexOf('\'') >= 0 || nativeType.indexOf('`') >= 0)
            return nativeType;
        String leading = nativeType.strip().split("[ (]", 2)[0].toUpperCase(java.util.Locale.ROOT);
        if (!java.util.Set.of("VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2", "CHAR", "NCHAR", "CHARACTER",
                "INTEGER", "INT", "SMALLINT", "BIGINT", "DECIMAL", "NUMERIC", "NUMBER", "REAL", "FLOAT", "DOUBLE",
                "DECFLOAT", "DATE", "TIME", "TIMESTAMP", "CLOB", "NCLOB", "DBCLOB", "BLOB", "BINARY", "VARBINARY",
                "GRAPHIC", "VARGRAPHIC", "LONG", "XML", "BOOLEAN").contains(leading)) return nativeType;
        return nativeType.toUpperCase(java.util.Locale.ROOT);
    }

}
