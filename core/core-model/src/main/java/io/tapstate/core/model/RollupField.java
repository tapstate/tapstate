package io.tapstate.core.model;

import java.util.ArrayList;
import java.util.List;

/**
 * One rollup field: a single aggregate function over the live elements of the level it is declared on,
 * written on that level's parent. Exactly one of the function components is present; it carries the
 * argument in the form it was written ({@code $column} or {@code =expression}, or {@code true} for a count
 * of elements). The remaining components qualify that one function.
 */
@Doc("A value computed over the live elements of this embed and written on its parent, beside the embed's "
        + "path. Exactly one function is given.")
public record RollupField(
        @Doc("Counts elements: true counts every element that passes the filter, an expression counts its "
                + "non-null values.")
        Object count,
        @Doc("Counts the distinct non-null values of an expression.")
        String countDistinct,
        @Doc("Sums a numeric expression. Integer and decimal sums are exact or refused with a code.")
        String sum,
        @Doc("Averages a numeric expression. An integer or decimal argument gives a decimal rounded half-even.")
        String avg,
        @Doc("The smallest value of a numeric, string or temporal expression.")
        String min,
        @Doc("The largest value of a numeric, string or temporal expression.")
        String max,
        @Doc("The value of the element with the smallest ordering.")
        RollupPick first,
        @Doc("The value of the element with the greatest ordering.")
        RollupPick last,
        @Doc("A predicate: an element contributes to this field only while it holds.")
        String filter,
        @Doc("Converts the result exactly to another type, refusing a conversion that is not exact.")
        RollupResultType type,
        @Doc(value = "The value rendered when no element contributes. It never replaces the null a missing "
                + "reference gives.", key = "default")
        Object defaultValue) {

    public RollupField {
        List<RollupFunction> given = functionsOf(count, countDistinct, sum, avg, min, max, first, last);
        if (given.size() != 1) {
            throw new IllegalArgumentException("a rollup field carries exactly one function, not " + given);
        }
        if (count != null && !(count instanceof String) && !Boolean.TRUE.equals(count)) {
            throw new IllegalArgumentException("count takes true or an expression");
        }
    }

    /** A field applying {@code function} to {@code argument}, which is a pick for first and last. */
    public static RollupField of(RollupFunction function, Object argument, String filter, RollupResultType type,
            Object defaultValue) {
        return switch (function) {
            case COUNT -> new RollupField(argument, null, null, null, null, null, null, null, filter, type,
                    defaultValue);
            case COUNT_DISTINCT -> new RollupField(null, (String) argument, null, null, null, null, null, null,
                    filter, type, defaultValue);
            case SUM -> new RollupField(null, null, (String) argument, null, null, null, null, null, filter, type,
                    defaultValue);
            case AVG -> new RollupField(null, null, null, (String) argument, null, null, null, null, filter, type,
                    defaultValue);
            case MIN -> new RollupField(null, null, null, null, (String) argument, null, null, null, filter, type,
                    defaultValue);
            case MAX -> new RollupField(null, null, null, null, null, (String) argument, null, null, filter, type,
                    defaultValue);
            case FIRST -> new RollupField(null, null, null, null, null, null, (RollupPick) argument, null, filter,
                    type, defaultValue);
            case LAST -> new RollupField(null, null, null, null, null, null, null, (RollupPick) argument, filter,
                    type, defaultValue);
        };
    }

    /** The one function this field applies. */
    public RollupFunction function() {
        return functionsOf(count, countDistinct, sum, avg, min, max, first, last).getFirst();
    }

    /**
     * What the function is applied to, as written: {@code Boolean.TRUE} for a count of elements, the
     * expression text for the value functions, and the {@link RollupPick} for first and last.
     */
    public Object argument() {
        return switch (function()) {
            case COUNT -> count;
            case COUNT_DISTINCT -> countDistinct;
            case SUM -> sum;
            case AVG -> avg;
            case MIN -> min;
            case MAX -> max;
            case FIRST -> first;
            case LAST -> last;
        };
    }

    private static List<RollupFunction> functionsOf(Object count, String countDistinct, String sum, String avg,
            String min, String max, RollupPick first, RollupPick last) {
        List<RollupFunction> given = new ArrayList<>();
        Object[] values = {count, countDistinct, sum, avg, min, max, first, last};
        RollupFunction[] functions = RollupFunction.values();
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                given.add(functions[i]);
            }
        }
        return given;
    }
}
