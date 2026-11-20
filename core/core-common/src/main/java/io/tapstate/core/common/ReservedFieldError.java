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
    RENAMED(SchemaErrorDefinitions.RESERVED_FIELD_RENAMED),

    /**
     * The name such a column would be read under already exists in the same table. {@code source} is
     * the connection it was found on, {@code table} the table, {@code renamed} the name in conflict.
     */
    RENAME_CONFLICT(SchemaErrorDefinitions.RESERVED_FIELD_RENAME_CONFLICT);

    private final ProductErrorDefinition definition;

    ReservedFieldError(ProductErrorDefinition definition) {
        this.definition = definition;
    }

    @Override
    public String code() {
        return definition.code();
    }

    @Override
    public Severity severity() {
        return definition.severity();
    }

    @Override
    public Set<String> placeholders() {
        return definition.parameters();
    }
}
