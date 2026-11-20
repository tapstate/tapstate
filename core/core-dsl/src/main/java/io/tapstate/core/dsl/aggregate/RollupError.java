package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The rollup refusals that need more than the YAML to judge: which way an embed points, what types the
 * level's columns and references resolved to, and what a value turned out to be at render. The refusals
 * the YAML alone settles are {@code dsl} codes raised where they are written.
 *
 * <p>Codes are an external contract and append-only.
 */
public enum RollupError implements TapstateErrorCode {

    /** Checking the tree: {@code rollup:} on an embed the level points at, which holds at most one row. */
    ON_REFERENCE_EMBED("nest.rollup-on-reference-embed", Set.of("embedPath")),

    /**
     * Checking the tree, or rendering: a rollup field's name is already taken on the level it lands on, by
     * a column, an embed path, a flat field or another rollup field.
     */
    FIELD_CONFLICT("nest.rollup-field-conflict", Set.of("field", "level", "other")),

    /** Checking the tree: an argument, ordering, filter, type or default does not type-check. */
    EXPRESSION_INVALID("nest.rollup-expression-invalid", Set.of("field", "part", "reason")),

    /** Checking the tree: a sum or average reads a value whose numeric type nothing resolved. */
    TYPE_UNKNOWN("nest.rollup-type-unknown", Set.of("field", "expression")),

    /** Rendering: an integer sum left 64 bits, or a decimal one needed more than 38 digits. */
    OVERFLOW("nest.rollup-overflow", Set.of("field", "root", "type")),

    /** Rendering: an element's value is not of the type its column was declared with. */
    VALUE_TYPE_MISMATCH("nest.rollup-value-type-mismatch", Set.of("field", "root", "declared", "actual")),

    /** Rendering: a {@code type:} conversion would round, truncate or overflow. */
    RESULT_NOT_CONVERTIBLE("nest.rollup-result-not-convertible", Set.of("field", "root", "from", "to")),

    /** Rendering: an argument, ordering or filter failed to evaluate on an element. */
    EVALUATION_FAILED("nest.rollup-evaluation-failed", Set.of("field", "root", "expression", "reason"));

    private final String code;
    private final Set<String> placeholders;

    RollupError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
