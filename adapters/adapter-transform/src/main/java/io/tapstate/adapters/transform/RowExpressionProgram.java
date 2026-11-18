package io.tapstate.adapters.transform;

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelLateFunctionBindings;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.tapstate.core.dsl.RowClock;
import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.UnaryOperator;

/**
 * One compiled row expression, ready to evaluate against an event. The compiler environment is the
 * validate layer's — the AST comes from {@link RowExpressions}, so what type-checks at validate
 * time is exactly what runs here. The program is built once (member-side, from the serializable
 * expression text) and evaluated per event; a compiled program is immutable and reused.
 *
 * <p>Evaluation binds the envelope as the expression root the same way the compiler declared it:
 * {@code op} as its wire symbol, {@code ts} / {@code src} as scalars, and {@code before} / {@code
 * after} / {@code schema} as maps (an absent map binds empty, so a present-field test is well
 * defined rather than a null dereference). The row's values already speak the tapstate value model,
 * having been carried into it where the columns' types were resolved; binding adds the representations
 * this language uses for null and bytes, which evaluation takes back off again.
 */
final class RowExpressionProgram {

    // A standard runtime plus the same-type temporal orderings the compiler declares. It is immutable and
    // shared: building the per-expression program is the only per-instance cost.
    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder()
            .addFunctionBindings(sameTypeOrderings())
            .addFunctionBindings(calendarArithmetic())
            .build();

    /**
     * The calendar functions that do not read the clock, bound once. A failure is a value the function cannot
     * take - a zone that is not one, a receiver that is not a date - and surfaces as an evaluation failure of
     * the expression rather than as a crash.
     */
    private static List<CelFunctionBinding> calendarArithmetic() {
        return List.of(
                CelFunctionBinding.from(RowClock.ADD_DAYS_OVERLOAD, Object.class, Long.class,
                        (date, days) -> calendar(() -> RowClock.addDays(date, days))),
                CelFunctionBinding.from(RowClock.ADD_MONTHS_OVERLOAD, Object.class, Long.class,
                        (date, months) -> calendar(() -> RowClock.addMonths(date, months))),
                CelFunctionBinding.from(RowClock.ADD_YEARS_OVERLOAD, Object.class, Long.class,
                        (date, years) -> calendar(() -> RowClock.addYears(date, years))),
                CelFunctionBinding.from(RowClock.TO_DATE_OVERLOAD, Object.class, String.class,
                        (value, zone) -> calendar(() -> RowClock.toDate(value, zone))));
    }

    /**
     * The functions that read the clock, bound for one evaluation at the instant it was read, so every clock
     * call in one expression sees the same moment.
     */
    private static CelLateFunctionBindings clockAt(Instant now) {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
        return CelLateFunctionBindings.from(
                CelFunctionBinding.from(RowClock.NOW_OVERLOAD, List.of(), args -> timestamp),
                CelFunctionBinding.from(RowClock.TODAY_OVERLOAD, String.class,
                        zone -> calendar(() -> RowClock.today(now, zone))));
    }

    private static Object calendar(java.util.function.Supplier<Object> computation) throws CelEvaluationException {
        try {
            return computation.get();
        } catch (IllegalArgumentException | java.time.DateTimeException refused) {
            throw new CelEvaluationException(refused.getMessage());
        }
    }

    /**
     * One runtime binding per ordering operator, serving every temporal type the compiler declares it
     * for. The compiler admits these only where both operands carry the same temporal column type, so at
     * runtime both are that column's own values and are ordered by the ordering those values carry. The
     * runtime program is built from the untyped expression, where every declared overload of an operator
     * is a candidate; binding one per operator keeps the dispatch unambiguous, and the type judgement has
     * already been made where the columns' types are known. A pair with no order in common - two different
     * classes, or a class with no natural order - fails the evaluation rather than being guessed at.
     */
    private static List<CelFunctionBinding> sameTypeOrderings() {
        List<CelFunctionBinding> bindings = new ArrayList<>();
        Set<String> bound = new HashSet<>();
        for (RowExpressions.SameTypeComparison comparison : RowExpressions.sameTypeComparisonOverloads()) {
            if (!bound.add(comparison.function())) {
                continue;
            }
            IntPredicate holds = switch (comparison.function()) {
                case "_<_" -> order -> order < 0;
                case "_<=_" -> order -> order <= 0;
                case "_>_" -> order -> order > 0;
                case "_>=_" -> order -> order >= 0;
                default -> throw new IllegalStateException("no ordering for " + comparison.function());
            };
            bindings.add(CelFunctionBinding.from(comparison.overloadId(), Object.class, Object.class,
                    (left, right) -> holds.test(order(left, right))));
        }
        return List.copyOf(bindings);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int order(Object left, Object right) throws CelEvaluationException {
        if (left instanceof Comparable comparable && right != null && left.getClass() == right.getClass()) {
            return comparable.compareTo(right);
        }
        throw new CelEvaluationException("cannot order a " + className(left) + " against a "
                + className(right));
    }

    private static String className(Object value) {
        return value == null ? "null" : value.getClass().getName();
    }

    private final CelRuntime.Program program;
    // The source expression, kept so an evaluation failure can name the expression that failed.
    private final String expr;
    // What now() reads: the processing host's clock, replaced only by a test that has to hold time still.
    private final Clock clock;

    private RowExpressionProgram(CelRuntime.Program program, String expr, Clock clock) {
        this.program = program;
        this.expr = expr;
        this.clock = clock;
    }

    /** Compiles a predicate (bool) expression into an evaluable program. */
    static RowExpressionProgram predicate(String expr) {
        return predicate(expr, Clock.systemUTC());
    }

    /** A predicate whose clock functions read {@code clock}. */
    static RowExpressionProgram predicate(String expr, Clock clock) {
        return of(RowExpressions.predicateAst(expr), expr, clock);
    }

    /** Compiles a computed-value expression (any type) into an evaluable program. */
    static RowExpressionProgram value(String expr) {
        return value(expr, Clock.systemUTC());
    }

    /** A computed value whose clock functions read {@code clock}. */
    static RowExpressionProgram value(String expr, Clock clock) {
        return of(RowExpressions.valueAst(expr), expr, clock);
    }

    private static RowExpressionProgram of(CelAbstractSyntaxTree ast, String expr, Clock clock) {
        try {
            return new RowExpressionProgram(RUNTIME.createProgram(ast), expr, clock);
        } catch (CelEvaluationException e) {
            // A checked AST builds into a program; a failure here is an invariant violation, not a
            // user condition.
            throw new IllegalStateException("row expression program could not be built", e);
        }
    }

    /** Evaluates the expression against one event, returning the raw CEL result. */
    Object eval(Envelope event) {
        return eval(event, event.after());
    }

    /** Evaluates a row image as {@code after}, keeping the original event's other bindings. */
    Object eval(Envelope event, Map<String, Object> after) {
        Map<String, Object> vars = new HashMap<>(8);
        vars.put("op", event.op().symbol());
        vars.put("ts", event.ts());
        vars.put("src", event.src());
        vars.put("before", bind(event.before()));
        vars.put("after", bind(after));
        vars.put("schema", bind(event.schema()));
        try {
            return unbound(program.eval(vars, clockAt(clock.instant())));
        } catch (CelEvaluationException e) {
            // A row-level evaluation failure (a missing field, a type clash on a dyn value, a function
            // that type-checks but is unbound at runtime) is a user-diagnosable condition: surface it
            // as a coded diagnostic naming the expression, not a bare crash that fails the job opaquely.
            throw TransformErrors.expressionFailed(expr, e);
        }
    }

    // A row image as the expression sees it. An absent map binds empty, so a present-field test is
    // well defined rather than a null dereference.
    @SuppressWarnings("unchecked")
    private static Map<String, Object> bind(Map<String, Object> row) {
        return row == null ? Map.of() : (Map<String, Object>) bound(row);
    }

    // The row already speaks the value model — its numbers were carried into it at the boundary that
    // resolved the columns' types, so an integral column is already the one integer width an
    // expression can do arithmetic on. Nothing here re-decides that: a second widening would be a
    // second opinion about what a column is, and the one thing it could add is a disagreement.
    //
    // What is left are the representations this language needs and the row does not have. A Java null
    // is the evaluator's marker for an unknown lookup, so a present null has to travel as CEL's null
    // value instead. Bytes travel as the language's own byte string. Both wrappers go on here and come
    // back off in unbound, so a sink is still owed the row's own values.
    //
    // Nested values are wrapped too, since a document's own fields and an array's elements are as
    // reachable from an expression as a top-level column. A container whose contents all pass through
    // unchanged is returned as it is, so the common row costs no copy.
    private static Object bound(Object value) {
        // A value a connector converted arrives in a carrier, which an expression can only compare by
        // identity: `after._id == "64f0..."` is then false for every row, and the expression neither
        // fails nor warns. The carrier is not restored on the way out - what an expression produces is
        // a new value, and it is only the row's own untouched values that keep travelling as they came.
        if (value instanceof ConvertedValue carried) {
            return bound(carried.value());
        }
        if (value == null) {
            return NullValue.NULL_VALUE;
        }
        if (value instanceof byte[] bytes) {
            return ByteString.copyFrom(bytes);
        }
        return mapped(value, RowExpressionProgram::bound);
    }

    // The reverse, for what an expression hands back. CEL null and the byte string are the expression
    // language's own wrappers; a sink is owed the row's values, so a value that merely travelled
    // through an expression leaves as the kind of value it arrived as.
    private static Object unbound(Object value) {
        if (value == NullValue.NULL_VALUE) {
            return null;
        }
        if (value instanceof ByteString bytes) {
            return bytes.toByteArray();
        }
        // A timestamp is the expression language's own representation of an instant; a sink is owed the
        // platform's, the one a source's date and time with an offset arrives as.
        if (value instanceof Timestamp timestamp) {
            return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        }
        return mapped(value, RowExpressionProgram::unbound);
    }

    // Applies a conversion through a map or a list, returning the original container when nothing
    // inside it changed. Any other value is its own conversion.
    private static Object mapped(Object value, UnaryOperator<Object> conversion) {
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> converted = new LinkedHashMap<>(map.size());
            boolean changed = false;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object element = conversion.apply(entry.getValue());
                changed |= element != entry.getValue();
                converted.put(entry.getKey(), element);
            }
            return changed ? converted : map;
        }
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            boolean changed = false;
            for (Object element : list) {
                Object next = conversion.apply(element);
                changed |= next != element;
                converted.add(next);
            }
            return changed ? converted : list;
        }
        return value;
    }
}
