package io.tapstate.core.dsl;

import dev.cel.common.CelErrorCode;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.Contribution;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.model.RollupFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One input row as an aggregate step sees it: whether the row is a member at all, which group it belongs
 * to, and what it gives each field. A pure function of the step and the row, so the same row evaluates the
 * same way on a snapshot, on a change and on a replay.
 *
 * <p>The rules are the function library's: a row whose field filter does not hold gives that field
 * nothing; a null argument gives a value function nothing; a count without an argument counts the row;
 * first and last take the row unless one of its ordering values is null, and take its value even when
 * that is null.
 */
public final class AggregateRow {

    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder().build();

    /** Built once per expression text; a compiled program is immutable and safe to share. */
    private static final Map<String, CelRuntime.Program> PROGRAMS = new ConcurrentHashMap<>();

    private AggregateRow() {
    }

    /**
     * What a member row comes to.
     *
     * @param group  the values of the group's key, in the order of the output key
     * @param inputs one entry per field in declared order; null where the row gives the field nothing
     */
    public record Evaluated(List<Object> group, List<Contribution> inputs) {
        public Evaluated {
            group = Collections.unmodifiableList(new ArrayList<>(group));
            inputs = Collections.unmodifiableList(new ArrayList<>(inputs));
        }
    }

    /** The row's identity: the values of the step's key columns, in order. */
    public static List<Object> key(AggregateStepSpec spec, Map<String, Object> row) {
        List<Object> key = new ArrayList<>(spec.key().size());
        for (String column : spec.key()) {
            key.add(row.get(column));
        }
        return key;
    }

    /** The row evaluated against the step, or empty when the step's condition does not hold for it. */
    public static Optional<Evaluated> of(AggregateStepSpec spec, Map<String, Object> row) {
        Map<String, Object> after = Map.of("after", bound(row));
        if (spec.where() != null && !Boolean.TRUE.equals(evaluate(spec, "where", spec.where(), true, after))) {
            return Optional.empty();
        }
        List<Object> group = new ArrayList<>(spec.groupBy().size());
        spec.groupBy().forEach((name, expression) ->
                group.add(evaluate(spec, "group `" + name + "`", expression, false, after)));
        List<Contribution> inputs = new ArrayList<>(spec.fields().size());
        for (RollupSpec field : spec.fields()) {
            inputs.add(input(spec, field, after));
        }
        return Optional.of(new Evaluated(group, inputs));
    }

    private static Contribution input(AggregateStepSpec spec, RollupSpec field, Map<String, Object> after) {
        String part = "field `" + field.field() + "`";
        if (field.filter() != null && !Boolean.TRUE.equals(evaluate(spec, part, field.filter(), true, after))) {
            return null;
        }
        if (field.function() == RollupFunction.FIRST || field.function() == RollupFunction.LAST) {
            List<Object> by = new ArrayList<>(field.by().size());
            for (String order : field.by()) {
                Object value = evaluate(spec, part, order, false, after);
                if (value == null) {
                    return null;
                }
                by.add(value);
            }
            return new Contribution(evaluate(spec, part, field.argument(), false, after), by, List.of());
        }
        if (field.argument() == null) {
            return Contribution.of(null, List.of());
        }
        Object value = evaluate(spec, part, field.argument(), false, after);
        return value == null ? null : Contribution.of(value, List.of());
    }

    /**
     * One expression over the row. A column the row does not carry reads as null, as a null column would:
     * the bound row leaves nulls out, so the two cannot be told apart and are not meant to be.
     */
    private static Object evaluate(AggregateStepSpec spec, String part, String expr, boolean predicate,
            Map<String, Object> after) {
        CelRuntime.Program program = PROGRAMS.computeIfAbsent((predicate ? "?" : "=") + expr, key -> {
            try {
                return RUNTIME.createProgram(RollupExpressions.evaluableAst(expr, predicate));
            } catch (CelEvaluationException e) {
                throw new IllegalStateException("a checked aggregate expression did not build: " + expr, e);
            }
        });
        try {
            return program.eval(after);
        } catch (CelEvaluationException e) {
            if (e.getErrorCode() == CelErrorCode.ATTRIBUTE_NOT_FOUND) {
                return null;
            }
            throw new TapstateException(AggregateError.EVALUATION_FAILED, Map.of("step", spec.step(),
                    "part", part, "expression", expr, "reason", String.valueOf(e.getMessage())), e);
        }
    }

    /** The row as an expression sees it: nulls left out, so a null and an absent column read alike. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> bound(Map<String, Object> row) {
        Map<String, Object> out = new HashMap<>(row.size() * 2);
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (value instanceof Map<?, ?> nested) {
                value = bound((Map<String, Object>) nested);
            } else if (value instanceof List<?> list) {
                List<Object> copy = new ArrayList<>(list.size());
                for (Object item : list) {
                    copy.add(item instanceof Map<?, ?> m ? bound((Map<String, Object>) m) : item);
                }
                value = copy;
            }
            out.put(entry.getKey(), value);
        }
        return out;
    }
}
