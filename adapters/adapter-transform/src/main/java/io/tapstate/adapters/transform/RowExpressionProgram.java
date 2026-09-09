package io.tapstate.adapters.transform;

import com.google.protobuf.ByteString;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * having been carried into it where the columns' types were resolved; the only thing binding adds is
 * the byte string this language uses for bytes, which evaluation takes back off again.
 */
final class RowExpressionProgram {

    // A standard runtime with no custom function bindings. It is immutable and shared: building the
    // per-expression program is the only per-instance cost.
    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder().build();

    private final CelRuntime.Program program;
    // The source expression, kept so an evaluation failure can name the expression that failed.
    private final String expr;

    private RowExpressionProgram(CelRuntime.Program program, String expr) {
        this.program = program;
        this.expr = expr;
    }

    /** Compiles a predicate (bool) expression into an evaluable program. */
    static RowExpressionProgram predicate(String expr) {
        return of(RowExpressions.predicateAst(expr), expr);
    }

    /** Compiles a computed-value expression (any type) into an evaluable program. */
    static RowExpressionProgram value(String expr) {
        return of(RowExpressions.valueAst(expr), expr);
    }

    private static RowExpressionProgram of(CelAbstractSyntaxTree ast, String expr) {
        try {
            return new RowExpressionProgram(RUNTIME.createProgram(ast), expr);
        } catch (CelEvaluationException e) {
            // A checked AST builds into a program; a failure here is an invariant violation, not a
            // user condition.
            throw new IllegalStateException("row expression program could not be built", e);
        }
    }

    /** Evaluates the expression against one event, returning the raw CEL result. */
    Object eval(Envelope event) {
        Map<String, Object> vars = new HashMap<>(8);
        vars.put("op", event.op().symbol());
        vars.put("ts", event.ts());
        vars.put("src", event.src());
        vars.put("before", bind(event.before()));
        vars.put("after", bind(event.after()));
        vars.put("schema", bind(event.schema()));
        try {
            return unbound(program.eval(vars));
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
    // What is left is the one representation this language needs and the row does not have. Bytes
    // travel as the language's own byte string, which is a wrapper rather than a value: it goes on
    // here and comes back off in unbound, so a sink is still owed the row's own bytes.
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
        if (value instanceof byte[] bytes) {
            return ByteString.copyFrom(bytes);
        }
        return mapped(value, RowExpressionProgram::bound);
    }

    // The reverse, for what an expression hands back. The byte string is the expression language's
    // own wrapper; a sink is owed the row's bytes, so a value that merely travelled through an
    // expression leaves as the kind of value it arrived as.
    private static Object unbound(Object value) {
        if (value instanceof ByteString bytes) {
            return bytes.toByteArray();
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
