package io.tapstate.core.dsl;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code schema} domain's authoring-time naming code: what the columns a batch would write have to
 * say to the author applying it. It is a warning — a batch carrying one is applied, and the pipeline it
 * describes runs and writes correct rows.
 *
 * <p>Kept apart from the codes that refuse a batch, and in a domain of its own, because nothing the
 * author wrote is wrong: the name came from the source's own schema, and both sides accept it. What is
 * reported is what that name will mean once it is a key in the store on the other side.
 *
 * <p>{@code placeholders()} is the named-argument contract: every raise site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum DocumentKeyError implements TapstateErrorCode {

    /**
     * A column whose own name holds a dot is written to a store that reads a dot in a key as a step
     * into a nested document. {@code column} is the name as the source reports it, {@code table} and
     * {@code source} where it comes from, {@code pipeline} what writes it, and {@code target} the
     * declared source the write lands in.
     */
    COLUMN_NAME_READS_AS_A_PATH(
            "schema.column-name-reads-as-a-path",
            Set.of("pipeline", "source", "table", "column", "target"));

    private final String code;
    private final Set<String> placeholders;

    DocumentKeyError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    /**
     * The rows are written correctly and the key is really there, so there is nothing here to refuse:
     * a batch turned away over this would also turn away every pipeline whose data already sits in a
     * collection this way.
     */
    @Override
    public Severity severity() {
        return Severity.WARNING;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
