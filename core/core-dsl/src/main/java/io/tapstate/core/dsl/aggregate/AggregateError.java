package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.Severity;
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
    INPUT_NOT_SUPPORTED("aggregate.input-not-supported", Set.of("step", "from", "kind")),

    /**
     * A filter step sits between the source and the step, and the source does not send the row as it was
     * before an update: a row that stops matching would stay in its group.
     */
    UPSTREAM_FILTER_UNSAFE("aggregate.upstream-filter-unsafe", Set.of("step", "filter", "source")),

    /** The input has no primary key and the step names no {@code key}. */
    KEY_REQUIRED("aggregate.key-required", Set.of("step", "from")),

    /** One name is used twice across the group names and the fields. */
    NAME_CONFLICT("aggregate.name-conflict", Set.of("step", "name")),

    /** A key column, group expression, predicate, argument, ordering, filter, type or default does not check. */
    EXPRESSION_INVALID("aggregate.expression-invalid", Set.of("step", "field", "part", "reason")),

    /** A sum or average reads a value whose numeric type nothing resolved. */
    TYPE_UNKNOWN("aggregate.type-unknown", Set.of("step", "field", "expression")),

    /** Running: an expression that passed its checks failed on a row. */
    EVALUATION_FAILED("aggregate.evaluation-failed", Set.of("step", "part", "expression", "reason")),

    /** Running: a group's sum is larger than its type holds; the group is held at its last good values. */
    OVERFLOW("aggregate.overflow", Set.of("step", "field", "group", "type")),

    /** Running: a row carries a value of another type than its column declares; the group is held. */
    VALUE_TYPE_MISMATCH("aggregate.value-type-mismatch", Set.of("step", "field", "group", "declared", "actual")),

    /** Running: a field's {@code type:} cannot hold its group's value exactly; the group is held. */
    RESULT_NOT_CONVERTIBLE("aggregate.result-not-convertible", Set.of("step", "field", "group", "from", "to")),

    /** The stored state was built by a different definition of the step. */
    DEFINITION_CHANGED("aggregate.definition-changed", Set.of("step", "changed")),

    /** A target fed by the step would keep its existing rows through a full load. */
    REBUILD_NEEDS_CLEAR("aggregate.rebuild-needs-clear", Set.of("step", "target", "policy")),

    /** A check found a value derived for a group that differs from what the group's stored rows give. */
    TOTAL_MISMATCH("aggregate.total-mismatch", Set.of("step", "group", "field")),

    /** A check or a repair was asked of a step that no member is running. */
    STEP_NOT_RUNNING("aggregate.step-not-running", Set.of("pipeline", "step")),

    /** A group named for a check or a repair does not read as a group of the step. */
    GROUP_INVALID("aggregate.group-invalid", Set.of("step", "reason"));

    /** The step's code for a reason the shared function library reports under its own name. */
    public static AggregateError of(RollupError reason) {
        return switch (reason) {
            case OVERFLOW -> OVERFLOW;
            case VALUE_TYPE_MISMATCH -> VALUE_TYPE_MISMATCH;
            case RESULT_NOT_CONVERTIBLE -> RESULT_NOT_CONVERTIBLE;
            default -> throw new IllegalArgumentException("not a reason a group is held for: " + reason);
        };
    }

    private final String code;
    private final Set<String> placeholders;

    AggregateError(String code, Set<String> placeholders) {
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
