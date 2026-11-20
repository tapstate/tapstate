package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.AggregateErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The refusals of an aggregate step that are judged before a row arrives: what it reads, how its input
 * rows are identified, and whether its names and expressions hold against the input's columns. A field
 * naming no known function is refused where the field is written, with the code a rollup field gets.
 *
 * <p>Codes are an external contract and append-only.
 */
public enum AggregateError implements TapstateErrorCode {

    /** The step reads something other than one source table or a filter, map or union over source tables. */
    INPUT_NOT_SUPPORTED(AggregateErrorDefinitions.INPUT_NOT_SUPPORTED),

    /**
     * A filter step sits between the source and the step, and the source does not send the row as it was
     * before an update: a row that stops matching would stay in its group.
     */
    UPSTREAM_FILTER_UNSAFE(AggregateErrorDefinitions.UPSTREAM_FILTER_UNSAFE),

    /** The input has no primary key and the step names no {@code key}. */
    KEY_REQUIRED(AggregateErrorDefinitions.KEY_REQUIRED),

    /** One name is used twice across the group names and the fields. */
    NAME_CONFLICT(AggregateErrorDefinitions.NAME_CONFLICT),

    /** A key column, group expression, predicate, argument, ordering, filter, type or default does not check. */
    EXPRESSION_INVALID(AggregateErrorDefinitions.EXPRESSION_INVALID),

    /** A sum or average reads a value whose numeric type nothing resolved. */
    TYPE_UNKNOWN(AggregateErrorDefinitions.TYPE_UNKNOWN),

    /** Running: an expression that passed its checks failed on a row. */
    EVALUATION_FAILED(AggregateErrorDefinitions.EVALUATION_FAILED),

    /** Running: a group's sum is larger than its type holds; the group is held at its last good values. */
    OVERFLOW(AggregateErrorDefinitions.OVERFLOW),

    /** Running: a row carries a value of another type than its column declares; the group is held. */
    VALUE_TYPE_MISMATCH(AggregateErrorDefinitions.VALUE_TYPE_MISMATCH),

    /** Running: a field's {@code type:} cannot hold its group's value exactly; the group is held. */
    RESULT_NOT_CONVERTIBLE(AggregateErrorDefinitions.RESULT_NOT_CONVERTIBLE),

    /** The stored state was built by a different definition of the step. */
    DEFINITION_CHANGED(AggregateErrorDefinitions.DEFINITION_CHANGED),

    /** A target fed by the step would keep its existing rows through a full load. */
    REBUILD_NEEDS_CLEAR(AggregateErrorDefinitions.REBUILD_NEEDS_CLEAR),

    /** A check found a value derived for a group that differs from what the group's stored rows give. */
    TOTAL_MISMATCH(AggregateErrorDefinitions.TOTAL_MISMATCH),

    /** A check or a repair was asked of a step that no member is running. */
    STEP_NOT_RUNNING(AggregateErrorDefinitions.STEP_NOT_RUNNING),

    /** A group named for a check or a repair does not read as a group of the step. */
    GROUP_INVALID(AggregateErrorDefinitions.GROUP_INVALID),

    /** A running step did not take or answer a check or a repair in the time allowed. */
    STEP_BUSY(AggregateErrorDefinitions.STEP_BUSY),

    /** The step runs, but failed while answering a check or a repair. */
    STEP_FAILED(AggregateErrorDefinitions.STEP_FAILED),

    /** The step declares more fields than one of its stored rows can account for. */
    TOO_MANY_FIELDS(AggregateErrorDefinitions.TOO_MANY_FIELDS);

    /** The step's code for a reason the shared function library reports under its own name. */
    public static AggregateError of(RollupError reason) {
        return switch (reason) {
            case OVERFLOW -> OVERFLOW;
            case VALUE_TYPE_MISMATCH -> VALUE_TYPE_MISMATCH;
            case RESULT_NOT_CONVERTIBLE -> RESULT_NOT_CONVERTIBLE;
            default -> throw new IllegalArgumentException("not a reason a group is held for: " + reason);
        };
    }

    private final ProductErrorDefinition definition;

    AggregateError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
