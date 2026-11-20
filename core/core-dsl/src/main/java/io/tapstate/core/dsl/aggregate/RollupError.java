package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.NestErrorDefinitions;
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
    ON_REFERENCE_EMBED(NestErrorDefinitions.ROLLUP_ON_REFERENCE_EMBED),

    /**
     * Checking the tree, or rendering: a rollup field's name is already taken on the level it lands on, by
     * a column, an embed path, a flat field or another rollup field.
     */
    FIELD_CONFLICT(NestErrorDefinitions.ROLLUP_FIELD_CONFLICT),

    /** Checking the tree: an argument, ordering, filter, type or default does not type-check. */
    EXPRESSION_INVALID(NestErrorDefinitions.ROLLUP_EXPRESSION_INVALID),

    /** Checking the tree: a sum or average reads a value whose numeric type nothing resolved. */
    TYPE_UNKNOWN(NestErrorDefinitions.ROLLUP_TYPE_UNKNOWN),

    /** Rendering: an integer sum left 64 bits, or a decimal one needed more than 38 digits. */
    OVERFLOW(NestErrorDefinitions.ROLLUP_OVERFLOW),

    /** Rendering: an element's value is not of the type its column was declared with. */
    VALUE_TYPE_MISMATCH(NestErrorDefinitions.ROLLUP_VALUE_TYPE_MISMATCH),

    /** Rendering: a {@code type:} conversion would round, truncate or overflow. */
    RESULT_NOT_CONVERTIBLE(NestErrorDefinitions.ROLLUP_RESULT_NOT_CONVERTIBLE),

    /** Rendering: an argument, ordering or filter failed to evaluate on an element. */
    EVALUATION_FAILED(NestErrorDefinitions.ROLLUP_EVALUATION_FAILED);

    private final ProductErrorDefinition definition;

    RollupError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
