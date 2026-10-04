package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Judges a batch's row expressions against the columns their sources hold. The offline layers check
 * an expression's syntax, its result type and its envelope names; this checks it against the data it
 * will actually run on, which is knowable only where a connection has been discovered.
 *
 * <p>Reading a row field therefore obliges the author to have discovered the sources that feed it.
 * Without that obligation the check would be dead weight in normal use: an author applies first and
 * discovers afterwards, so there would be no model to judge against and every expression would pass
 * unexamined. An expression that reads no row field carries no obligation at all.
 *
 * <p>The obligation is scoped to what can actually reach the expression, followed back through the
 * pipeline's own wiring rather than taken as everything the pipeline reads. Demanding an unrelated
 * source be discovered would be sending the author to do something with no bearing on the expression
 * being judged. Where the wiring cannot be resolved offline — a regex selecting tables that cannot be
 * enumerated without a connection — every source the pipeline reads is required, since that is the
 * only answer that cannot let an undiscovered one through.
 *
 * <p><b>A table is judged against its own columns, never a pooled view.</b> The wiring names the
 * tables an expression reads, so each is judged on what it holds; where an expression genuinely reads
 * several — a {@code union}, a regex the connection has to resolve — it is judged against each of
 * them in turn and must hold for every one. Pooling them instead would have to call a column two
 * tables type differently unresolved, which is the ordinary shape of a real database rather than a
 * corner of it, and would refuse expressions that are perfectly correct about the table they read.
 * Judging each in turn refuses the same genuine conflicts while naming the table that caused one.
 *
 * <p>Each table's types follow the preceding maps on its own wiring path. An expression reads the
 * columns those maps produced, rather than the original columns discovery recorded.
 *
 * <p>Once the columns are known, three outcomes, split because each has a different next step for the
 * author: a column whose type cannot survive the operation is refused as unsupported; a column
 * nothing resolved a type for is refused as unknown, because allowing it would mean guessing; and an
 * expression that is simply wrong about a well-resolved column is the ordinary illegal-expression
 * diagnostic, since the offline layer would say the same thing if it had the types.
 *
 * <p>A column absent from the table's model is deliberately not refused: it stays untyped and passes,
 * because for a source whose model comes from sampling, a column the model missed is a normal state
 * rather than an error. A table absent from the model reads the same way, for the same reason.
 */
public final class RowExpressionTypeRules {

    private final Map<String, Resource> byId = new LinkedHashMap<>();
    private final Map<String, List<DiscoveredTable>> tablesBySource;

    private RowExpressionTypeRules(
            Collection<Resource> batch, Map<String, List<DiscoveredTable>> tablesBySource) {
        for (Resource r : batch) {
            byId.putIfAbsent(r.id(), r);
        }
        this.tablesBySource = tablesBySource;
    }

    /**
     * Validates every pipeline's row expressions against {@code tablesBySource} — the tables each
     * source was discovered to hold, keyed by the source's id — and throws on the first violation. A
     * source the map omits has not been discovered; one it maps to an empty list has been discovered
     * and holds nothing, which is a different thing and is not refused.
     */
    public static void validate(
            Collection<Resource> batch, Map<String, List<DiscoveredTable>> tablesBySource) {
        RowExpressionTypeRules rules = new RowExpressionTypeRules(batch, tablesBySource);
        for (Resource r : batch) {
            if (r instanceof PipelineResource p) {
                rules.validatePipeline(p);
            }
        }
    }

    private void validatePipeline(PipelineResource p) {
        Wiring wiring = new Wiring(p, byId);
        List<Step> transforms = p.transforms();
        if (transforms != null) {
            for (int i = 0; i < transforms.size(); i++) {
                Step step = transforms.get(i);
                validateStep(step, wiring.routesReaching(step.from()), "transforms[" + i + "]");
            }
        }
        validateServe(p.serve(), wiring);
    }

    // ---- where an expression can sit -----------------------------------------------------

    private void validateStep(Step step, List<Wiring.Route> upstream, String path) {
        switch (step) {
            case Step.Inline inline -> validateBody(inline.body(), upstream, path);
            // The body lives in the referenced definition, but a definition on its own reads no
            // source: it is judged here, where a pipeline gives it one.
            case Step.Use use -> {
                if (byId.get(use.use()) instanceof TransformResource definition) {
                    validateBody(definition.body(), upstream, path + ".use");
                }
            }
        }
    }

    private void validateBody(TransformBody body, List<Wiring.Route> upstream, String path) {
        switch (body) {
            case TransformBody.Filter filter -> judgePredicate(filter.expr(), upstream, path + ".expr");
            case TransformBody.MapProjection projection ->
                    validateFieldRules(projection.fields(), upstream, path + ".fields");
            default -> {
                // the remaining bodies carry no row expression
            }
        }
    }

    /**
     * A serve block's expressions read what serve is wired to read. Its sink elements each name a
     * source too, but that is the connection being written to or pushed at, never where the rows
     * being shaped came from.
     */
    private void validateServe(ServeBlock serve, Wiring wiring) {
        switch (serve) {
            case null -> {
            }
            case ServeBlock.Inline inline ->
                    validatePush(inline.push(), wiring.routesReaching(inline.from()), "serve");
            case ServeBlock.Use use -> {
                if (byId.get(use.use()) instanceof ServeResource definition) {
                    validatePush(definition.push(), wiring.routesReaching(use.from()), "serve.use");
                }
            }
        }
    }

    private void validatePush(List<PushElement> push, List<Wiring.Route> upstream, String path) {
        if (push == null) {
            return;
        }
        for (int i = 0; i < push.size(); i++) {
            PushFormat format = push.get(i).format();
            String at = path + ".push[" + i + "].format";
            switch (format) {
                case null -> {
                }
                case PushFormat.Cel cel -> judgeValue(cel.expr(), upstream, at);
                case PushFormat.Fields fields -> validateFieldRules(fields.fields(), upstream, at);
            }
        }
    }

    private void validateFieldRules(
            Map<String, FieldRule> fields, List<Wiring.Route> upstream, String path) {
        fields.forEach((name, rule) -> {
            if (rule instanceof FieldRule.Computed computed) {
                judgeValue(computed.celExpr(), upstream, path + "." + name);
            }
        });
    }

    // ---- the judgment --------------------------------------------------------------------

    private void judgePredicate(String expr, List<Wiring.Route> upstream, String path) {
        Set<String> referenced = RowExpressions.rowColumns(expr);
        if (referenced.isEmpty()) {
            return;     // reads no row data, so no source model bears on it
        }
        for (DiscoveredTable table : tablesReaching(expr, upstream, path)) {
            judgeTypes(expr, referenced, table, path,
                    RowExpressions.typedPredicateError(expr, table.columns()));
        }
    }

    private void judgeValue(String expr, List<Wiring.Route> upstream, String path) {
        Set<String> referenced = RowExpressions.rowColumns(expr);
        if (referenced.isEmpty()) {
            return;     // reads no row data, so no source model bears on it
        }
        for (DiscoveredTable table : tablesReaching(expr, upstream, path)) {
            judgeTypes(expr, referenced, table, path,
                    RowExpressions.typedValueError(expr, table.columns()));
        }
    }

    /**
     * Enforces the discovery obligation and returns every discovered table the expression can run on,
     * each to be judged on its own columns.
     *
     * <p>An upstream naming a table the model does not carry contributes nothing rather than falling
     * back to the source's other tables: the fallback is exactly the pooled view this check exists
     * without, and it would let a table the expression never reads decide the fate of one it does.
     */
    private List<DiscoveredTable> tablesReaching(String expr, List<Wiring.Route> upstream, String path) {
        List<DiscoveredTable> reached = new ArrayList<>();
        for (Wiring.Route route : upstream) {
            Upstream up = route.upstream();
            List<DiscoveredTable> discovered = tablesBySource.get(up.source());
            if (discovered == null) {
                // Reported as undiscovered rather than as columns with no resolved type: both would
                // refuse, but only this one tells the author what to do next.
                throw error(DslError.ROW_EXPRESSION_NEEDS_DISCOVERY, path,
                        Map.of("expr", expr, "source", up.source()));
            }
            for (DiscoveredTable table : discovered) {
                if (up.table() == null || up.table().equals(table.name())) {
                    reached.add(projectedTable(table, route.steps()));
                }
            }
        }
        return reached;
    }

    private DiscoveredTable projectedTable(DiscoveredTable table, List<Step> steps) {
        Map<String, TapstateType> columns = table.columns();
        for (Step step : steps) {
            TransformBody body = switch (step) {
                case Step.Inline inline -> inline.body();
                case Step.Use use -> byId.get(use.use()) instanceof TransformResource definition
                        ? definition.body() : null;
            };
            if (body instanceof TransformBody.MapProjection projection) {
                columns = projectedTypes(projection.fields(), columns);
            }
        }
        return new DiscoveredTable(table.name(), columns);
    }

    /** A map reads its input for every rule, then carries the unlisted, unconsumed fields onward. */
    private static Map<String, TapstateType> projectedTypes(
            Map<String, FieldRule> rules, Map<String, TapstateType> input) {
        Map<String, TapstateType> output = new LinkedHashMap<>();
        Set<String> consumed = new LinkedHashSet<>();
        Set<String> dropped = new LinkedHashSet<>();
        rules.forEach((name, rule) -> {
            switch (rule) {
                case FieldRule.Rename rename -> {
                    consumed.add(rename.sourceField());
                    if (input.containsKey(rename.sourceField())) {
                        output.put(name, input.get(rename.sourceField()));
                    }
                }
                case FieldRule.Drop() -> dropped.add(name);
                case FieldRule.Literal literal -> output.put(name, literalType(literal.value()));
                case FieldRule.Computed computed ->
                        output.put(name, RowExpressions.typedValueType(computed.celExpr(), input));
                default -> throw new IllegalStateException("Unexpected field rule: " + rule);
            }
        });
        input.forEach((name, type) -> {
            if (!output.containsKey(name) && !consumed.contains(name) && !dropped.contains(name)) {
                output.put(name, type);
            }
        });
        return output;
    }

    private static TapstateType literalType(Object value) {
        if (value instanceof Boolean) {
            return TapstateType.BOOLEAN;
        }
        if (value instanceof String) {
            return TapstateType.STRING;
        }
        if (value instanceof Integer || value instanceof Long) {
            return TapstateType.INT64;
        }
        if (value instanceof Double) {
            return TapstateType.DOUBLE;
        }
        return TapstateType.UNKNOWN;
    }

    private void judgeTypes(String expr, Set<String> referenced, DiscoveredTable table,
            String path, String compileError) {
        // A column nothing resolved is refused however it is used - even a presence test - because
        // what may be done with it is exactly what is not known.
        for (String column : referenced) {
            if (table.columns().get(column) == TapstateType.UNKNOWN) {
                throw error(DslError.ROW_EXPRESSION_TYPE_UNKNOWN, path,
                        Map.of("expr", expr, "column", column, "table", table.name()));
            }
        }
        if (compileError == null) {
            return;
        }
        // The expression does not check out. Naming the column whose type has no exact counterpart
        // separates "this column cannot be computed on" from "this expression is wrong", which are
        // different problems with different fixes.
        for (String column : referenced) {
            TapstateType type = table.columns().get(column);
            if (type != null && RowExpressions.withoutExactCelType(type)) {
                throw error(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED, path, Map.of(
                        "expr", expr, "column", column, "type", type.name(), "table", table.name()));
            }
        }
        throw error(DslError.ILLEGAL_EXPRESSION, path, Map.of("expr", expr, "detail", compileError));
    }

    private static DslException error(DslError code, String path, Map<String, Object> args) {
        return new DslException(code, path, 0, 0, null, args);
    }
}
