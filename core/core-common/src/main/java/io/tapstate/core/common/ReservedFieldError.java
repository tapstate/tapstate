package io.tapstate.core.common;

import java.util.Set;

/**
 * What a source column carrying the product's reserved field name has to say: that it is known by
 * another name from capture onwards, or that the other name is taken too.
 *
 * <p>The rename is a warning because nothing is wrong and the pipeline runs: the column crosses with
 * every value intact. It is said at all because the target ends up with a field name its owner did
 * not write. Only the second collision stops anything, since then there is no name left to give the
 * column without the author choosing one.
 */
public enum ReservedFieldError implements TapstateErrorCode {

    /**
     * A source column named like the reserved field is read under {@code renamed}. {@code source} and
     * {@code table} say where it is, {@code column} the name the source gives it.
     */
    RENAMED("schema.reserved-field-renamed", Severity.WARNING,
            Set.of("source", "table", "column", "renamed")),

    /**
     * The name such a column would be read under already exists in the same table. {@code source} is
     * the connection it was found on, {@code table} the table, {@code renamed} the name in conflict.
     */
    RENAME_CONFLICT("schema.reserved-field-rename-conflict", Severity.ERROR,
            Set.of("source", "table", "renamed"));

    private final String code;
    private final Severity severity;
    private final Set<String> placeholders;

    ReservedFieldError(String code, Severity severity, Set<String> placeholders) {
        this.code = code;
        this.severity = severity;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return severity;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
