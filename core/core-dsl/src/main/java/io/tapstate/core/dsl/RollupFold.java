package io.tapstate.core.dsl;

import dev.cel.common.CelErrorCode;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.aggregate.Accumulator;
import io.tapstate.core.dsl.aggregate.AggregateFailure;
import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.Contribution;
import io.tapstate.core.dsl.aggregate.RollupElement;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.Values;
import io.tapstate.core.model.RollupFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes a level's rollup fields from its live elements as rendered. This is the whole of a rollup at
 * render: a pure function of the level's fields and its elements, called the same way wherever a document
 * is rendered, so a document built from a snapshot and the same document maintained by changes carry the
 * same values.
 *
 * <p>Every field of the level is folded in one walk over the elements, which are taken in element-key order
 * so a double sum adds in the same order on every render. A field whose expressions read a reference that an
 * element names but that rendered absent is null - incomplete - and its {@code default:} does not stand in;
 * a default stands in only when no element contributed at all.
 */
public final class RollupFold {

    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder().build();

    /** Built once per expression text; a compiled program is immutable and safe to share. */
    private static final Map<String, CelRuntime.Program> PROGRAMS = new ConcurrentHashMap<>();

    private RollupFold() {
    }

    /**
     * The values of {@code specs} over {@code elements}, keyed by field in declaration order. {@code root}
     * names the document in a refusal.
     */
    public static Map<String, Object> apply(List<RollupSpec> specs, List<RollupElement> elements, String root) {
        Map<String, Object> results = new LinkedHashMap<>();
        if (specs.isEmpty()) {
            return results;
        }
        List<RollupElement> ordered = new ArrayList<>(elements);
        ordered.sort((a, b) -> Values.compareKeys(a.key(), b.key()));
        int n = specs.size();
        Accumulator[] accumulators = new Accumulator[n];
        boolean[] incomplete = new boolean[n];
        for (int i = 0; i < n; i++) {
            accumulators[i] = Aggregates.of(specs.get(i).function()).init(specs.get(i).argumentType());
        }
        for (RollupElement element : ordered) {
            Map<String, Object> row = bound(element.rendered());
            for (int i = 0; i < n; i++) {
                RollupSpec spec = specs.get(i);
                if (incomplete[i]) {
                    continue;
                }
                if (!Collections.disjoint(element.missingReferences(), spec.references())) {
                    incomplete[i] = true;
                    continue;
                }
                try {
                    contribute(spec, accumulators[i], element, row, root);
                } catch (AggregateFailure failure) {
                    throw coded(failure, spec, root);
                }
            }
        }
        for (int i = 0; i < n; i++) {
            RollupSpec spec = specs.get(i);
            Object value;
            if (incomplete[i]) {
                value = null;
            } else if (!accumulators[i].contributed() && spec.defaultValue() != null) {
                value = spec.defaultValue();
            } else {
                try {
                    value = Values.convert(accumulators[i].result(), spec.convert());
                } catch (AggregateFailure failure) {
                    throw coded(failure, spec, root);
                }
            }
            results.put(spec.field(), value);
        }
        return results;
    }

    private static void contribute(RollupSpec spec, Accumulator accumulator, RollupElement element,
            Map<String, Object> row, String root) {
        if (spec.filter() != null && !Boolean.TRUE.equals(evaluate(spec, spec.filter(), true, row, root))) {
            return;
        }
        if (spec.function() == RollupFunction.FIRST || spec.function() == RollupFunction.LAST) {
            List<Object> by = new ArrayList<>(spec.by().size());
            for (String order : spec.by()) {
                by.add(evaluate(spec, order, false, row, root));
            }
            accumulator.add(new Contribution(evaluate(spec, spec.argument(), false, row, root), by, element.key()));
            return;
        }
        if (spec.argument() == null) {
            accumulator.add(Contribution.of(Boolean.TRUE, element.key()));
            return;
        }
        Object value = evaluate(spec, spec.argument(), false, row, root);
        if (value != null) {
            accumulator.add(Contribution.of(value, element.key()));
        }
    }

    /**
     * One expression over one element. A field the element does not carry reads as null, as a null column
     * would: the element map leaves nulls out, so the two cannot be told apart and are not meant to be.
     */
    private static Object evaluate(RollupSpec spec, String expr, boolean predicate, Map<String, Object> row,
            String root) {
        CelRuntime.Program program = PROGRAMS.computeIfAbsent((predicate ? "?" : "=") + expr, key -> {
            try {
                return RUNTIME.createProgram(RollupExpressions.evaluableAst(expr, predicate));
            } catch (CelEvaluationException e) {
                throw new IllegalStateException("a checked rollup expression did not build: " + expr, e);
            }
        });
        try {
            return program.eval(Map.of("after", row));
        } catch (CelEvaluationException e) {
            if (e.getErrorCode() == CelErrorCode.ATTRIBUTE_NOT_FOUND) {
                return null;
            }
            throw new TapstateException(RollupError.EVALUATION_FAILED, Map.of("field", spec.field(),
                    "root", root, "expression", expr, "reason", String.valueOf(e.getMessage())), e);
        }
    }

    private static TapstateException coded(AggregateFailure failure, RollupSpec spec, String root) {
        Map<String, Object> args = new HashMap<>(failure.args());
        args.put("field", spec.field());
        args.put("root", root);
        return new TapstateException(failure.code(), args, null);
    }

    /** The element as an expression sees it: nulls left out, so a null and an absent field read alike. */
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
