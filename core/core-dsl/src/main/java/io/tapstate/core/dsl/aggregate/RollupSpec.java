package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.RollupResultType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One rollup field, checked and ready to fold: the function, its expressions as row expressions over the
 * rendered element ({@code after}), the types fixed when the pipeline was applied, the typed default, and the
 * references whose absence makes the field incomplete. It travels with the job, so it holds text and
 * values, never a compiled program.
 *
 * @param argument   the argument expression, or null for a count of elements; for first and last, the value
 * @param by         first and last only: the ordering expressions, compared left to right
 * @param filter     the predicate an element must satisfy to contribute, or null
 * @param argumentType what the argument was declared to be; the fold refuses a value of another type
 * @param resultType the type of the field's value after any {@code type:} conversion
 * @param references the reference paths the field's expressions read
 */
public record RollupSpec(
        String field,
        RollupFunction function,
        String argument,
        List<String> by,
        String filter,
        ValueType argumentType,
        ValueType resultType,
        RollupResultType convert,
        Object defaultValue,
        Set<String> references) implements Serializable {

    private static final long serialVersionUID = 1L;

    public RollupSpec {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(function, "function");
        Objects.requireNonNull(argumentType, "argumentType");
        Objects.requireNonNull(resultType, "resultType");
        by = by == null ? List.of() : List.copyOf(by);
        references = references == null ? Set.of() : Set.copyOf(references);
    }
}
