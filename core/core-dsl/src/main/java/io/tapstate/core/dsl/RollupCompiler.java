package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.AggregateFunction;
import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RollupScope;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.dsl.aggregate.Values;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.RollupPick;
import io.tapstate.core.model.RollupResultType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks one rollup field against the element it folds over and fixes its types: the argument's, which every
 * value at render must match, and the result's, which a {@code default:} must already have. Everything that
 * can be judged before a row arrives is judged here, so what is left at render is arithmetic.
 */
public final class RollupCompiler {

    private RollupCompiler() {
    }

    /** {@code field} named {@code name}, checked over {@code scope}; refuses with a code when it cannot apply. */
    public static RollupSpec compile(String name, RollupField field, RollupScope scope) {
        RollupFunction function = field.function();
        AggregateFunction library = Aggregates.of(function);
        Set<String> references = new LinkedHashSet<>();
        String argument = null;
        List<String> by = new ArrayList<>();
        ValueType argumentType;
        if (field.argument() instanceof RollupPick pick) {
            argument = RollupExpressions.expressionOf(pick.value());
            argumentType = typed(name, "value", argument, scope, false, references);
            for (String written : pick.by()) {
                String expr = RollupExpressions.expressionOf(written);
                ValueType order = typed(name, "by", expr, scope, false, references);
                if (!order.comparable()) {
                    throw invalid(name, "by", written + " is " + order.spelled() + ", which has no order");
                }
                by.add(expr);
            }
        } else if (Boolean.TRUE.equals(field.argument())) {
            argumentType = ValueType.INT64;
        } else {
            String written = (String) field.argument();
            argument = RollupExpressions.expressionOf(written);
            argumentType = typed(name, "argument", argument, scope, false, references);
            if ((function == RollupFunction.SUM || function == RollupFunction.AVG)
                    && argumentType.type() == TapstateType.UNKNOWN) {
                throw new TapstateException(RollupError.TYPE_UNKNOWN,
                        Map.of("field", name, "expression", written), null);
            }
        }
        String filter = null;
        if (field.filter() != null) {
            filter = RollupExpressions.expressionOf(field.filter());
            typed(name, "filter", filter, scope, true, references);
        }
        ValueType resultType;
        try {
            resultType = library.resultType(argumentType);
        } catch (IllegalArgumentException refused) {
            throw invalid(name, "argument", refused.getMessage());
        }
        resultType = converted(name, resultType, field.type());
        Object defaultValue;
        try {
            defaultValue = Values.typedDefault(field.defaultValue(), resultType);
        } catch (IllegalArgumentException refused) {
            throw invalid(name, "default", refused.getMessage());
        }
        return new RollupSpec(name, function, argument, by, filter, argumentType, resultType, field.type(),
                defaultValue, references);
    }

    private static ValueType typed(String name, String part, String expr, RollupScope scope, boolean predicate,
            Set<String> references) {
        RollupExpressions.Typed typed = RollupExpressions.typed(expr, scope, predicate);
        if (typed.error() != null) {
            throw invalid(name, part, typed.error());
        }
        references.addAll(RollupExpressions.referencesRead(expr, scope));
        return typed.type();
    }

    /**
     * The result type after {@code type:}. Only a number converts, and only to a number; whether one value
     * converts exactly is a question for render, since a decimal sum is integral or not by its value.
     */
    private static ValueType converted(String name, ValueType result, RollupResultType to) {
        if (to == null) {
            return result;
        }
        if (!result.numeric()) {
            throw invalid(name, "type", "the result is " + result.spelled() + ", and only a number converts");
        }
        return switch (to) {
            case INT64 -> ValueType.INT64;
            case DOUBLE -> ValueType.DOUBLE;
            case DECIMAL -> result.type() == TapstateType.DECIMAL ? result
                    : ValueType.decimal(result.type() == TapstateType.INT64 ? 0 : null);
        };
    }

    private static TapstateException invalid(String name, String part, String reason) {
        return new TapstateException(RollupError.EXPRESSION_INVALID,
                Map.of("field", name, "part", part, "reason", firstLine(reason)), null);
    }

    private static String firstLine(String reason) {
        if (reason == null) {
            return "";
        }
        int end = reason.indexOf('\n');
        return (end < 0 ? reason : reason.substring(0, end)).trim();
    }
}
