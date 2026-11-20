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
 * @param elementBoundary how many elements its level should hold per document, for the level's row width
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
        Set<String> references,
        long elementBoundary) implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A field computed outside any nest level, such as an aggregate step's: it renders no document, so no
     * element boundary applies to it.
     */
    public RollupSpec(String field, RollupFunction function, String argument, List<String> by, String filter,
            ValueType argumentType, ValueType resultType, RollupResultType convert, Object defaultValue,
            Set<String> references) {
        this(field, function, argument, by, filter, argumentType, resultType, convert, defaultValue, references,
                Long.MAX_VALUE);
    }

    public RollupSpec {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(function, "function");
        Objects.requireNonNull(argumentType, "argumentType");
        Objects.requireNonNull(resultType, "resultType");
        by = by == null ? List.of() : List.copyOf(by);
        references = references == null ? Set.of() : Set.copyOf(references);
    }
}
