package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RollupScope;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What an aggregate step is held to before it runs.
 *
 * <p>Two moments. {@link #validate} judges the batch as written: what the step reads and whether its
 * names collide, neither of which needs a discovered model. {@link #compile} judges the step against the
 * columns that reach it, fixes every type, and gives the shape of its output rows.
 *
 * <p><b>Why the input is limited to a source table or a filter, map or union over source tables.</b> The
 * step keeps the last row it saw for each input key and compares positions to decide which of two events
 * is newer. Each event from a table or from a step that handles one row at a time carries a single
 * position. A document assembled from several tables carries a set of them, and which of two such
 * documents is newer is a different question, not answered here.
 */
public final class AggregateStepRules {

    private AggregateStepRules() {
    }

    /** Refuses every aggregate step in {@code batch} whose input or names cannot stand. */
    public static void validate(Collection<Resource> batch) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : batch) {
            byId.put(resource.id(), resource);
        }
        for (Resource resource : batch) {
            if (!(resource instanceof PipelineResource pipeline) || pipeline.transforms() == null) {
                continue;
            }
            Map<String, Step> steps = new LinkedHashMap<>();
            pipeline.transforms().forEach(step -> steps.put(step.id(), step));
            for (Step step : pipeline.transforms()) {
                if (bodyOf(step, byId) instanceof TransformBody.Aggregate aggregate) {
                    refuseNameConflicts(step.id(), aggregate);
                    refuseUnsupportedInput(step, steps, byId);
                    refuseFiltersUpstream(step.id(), pipeline, byId);
                }
            }
            refuseTargetsThatKeepRows(pipeline, steps, byId);
        }
    }

    /**
     * A filter step above the aggregate is refused. A filter handles one event at a time, so it can turn
     * "this row stopped matching" into a removal only when the update says what the row was before, and
     * no source declares that it always does. Without it the row's old contribution would stay in its
     * group for good. The step's own {@code where:} has no such limit, because the step remembers the row.
     */
    private static void refuseFiltersUpstream(String step, PipelineResource pipeline, Map<String, Resource> byId) {
        List<String> filters = filtersUpstream(step, pipeline, byId);
        if (!filters.isEmpty()) {
            throw new TapstateException(AggregateError.UPSTREAM_FILTER_UNSAFE, Map.of("step", step,
                    "filter", filters.getFirst(), "source", String.join(", ", pipeline.sourceIds())), null);
        }
    }

    /**
     * A target that reads an aggregate step may not keep its rows through a full load. A full load builds
     * every group again from the rows the source holds now, and a group whose rows are gone is not among
     * them: the step cannot name it once its old state is cleared, so nothing would ever delete its row.
     */
    private static void refuseTargetsThatKeepRows(PipelineResource pipeline, Map<String, Step> steps,
            Map<String, Resource> byId) {
        if (pipeline.view() != null) {
            FromRef from = switch (pipeline.view()) {
                case ViewBlock.Inline inline -> inline.from();
                case ViewBlock.Use use -> use.from();
            };
            OnFullLoad policy = switch (pipeline.view()) {
                case ViewBlock.Inline inline -> inline.onFullLoad();
                case ViewBlock.Use use -> byId.get(use.use()) instanceof ViewResource definition
                        ? definition.onFullLoad()
                        : null;
            };
            String id = switch (pipeline.view()) {
                case ViewBlock.Inline inline -> inline.id();
                case ViewBlock.Use use -> use.id() != null ? use.id() : use.use();
            };
            String step = from == null ? null : aggregateAbove(List.of(from), steps, byId, new LinkedHashSet<>());
            refuseKeeping(step, id, policy);
        }
        if (pipeline.serve() != null) {
            FromClause from = switch (pipeline.serve()) {
                case ServeBlock.Inline inline -> inline.from();
                case ServeBlock.Use use -> use.from();
            };
            List<SyncElement> sync = switch (pipeline.serve()) {
                case ServeBlock.Inline inline -> inline.sync();
                case ServeBlock.Use use -> byId.get(use.use()) instanceof ServeResource definition
                        ? definition.sync()
                        : null;
            };
            if (from == null || sync == null) {
                return;
            }
            String step = aggregateAbove(refsOf(from), steps, byId, new LinkedHashSet<>());
            for (SyncElement element : sync) {
                refuseKeeping(step, element.id() != null ? element.id() : element.source(), element.onFullLoad());
            }
        }
    }

    private static void refuseKeeping(String step, String target, OnFullLoad policy) {
        OnFullLoad effective = policy == null ? OnFullLoad.APPEND : policy;
        if (step != null && effective == OnFullLoad.APPEND) {
            throw new TapstateException(AggregateError.REBUILD_NEEDS_CLEAR,
                    Map.of("step", step, "target", String.valueOf(target), "policy", effective.yaml()), null);
        }
    }

    /** The nearest aggregate step that {@code refs} read, directly or through other steps, or null. */
    private static String aggregateAbove(Collection<FromRef> refs, Map<String, Step> steps,
            Map<String, Resource> byId, Set<String> visited) {
        for (FromRef ref : refs) {
            if (!(ref instanceof FromRef.Literal literal) || steps.get(literal.ref()) == null
                    || !visited.add(literal.ref())) {
                continue;
            }
            Step upstream = steps.get(literal.ref());
            if (bodyOf(upstream, byId) instanceof TransformBody.Aggregate) {
                return upstream.id();
            }
            String above = aggregateAbove(refsOf(upstream.from()), steps, byId, visited);
            if (above != null) {
                return above;
            }
        }
        return null;
    }

    /** The filter steps between {@code step} and its source tables, nearest first. */
    public static List<String> filtersUpstream(String step, PipelineResource pipeline, Map<String, Resource> byId) {
        Map<String, Step> steps = new LinkedHashMap<>();
        if (pipeline.transforms() != null) {
            pipeline.transforms().forEach(s -> steps.put(s.id(), s));
        }
        List<String> filters = new ArrayList<>();
        collectFilters(steps.get(step), steps, byId, filters, new LinkedHashSet<>());
        return filters;
    }

    /**
     * {@code aggregate}, the body of {@code step}, checked against the columns that reach it.
     *
     * @param input    the input's columns and their types
     * @param known    whether {@code input} came from a discovered model; where it did not, a column it
     *                 does not list is read as an untyped value
     * @param inputKey the input's own key, empty where it has none
     * @param from     how the input is named in a refusal
     */
    public static AggregateStepSpec compile(String step, TransformBody.Aggregate aggregate,
            Map<String, ValueType> input, boolean known, List<String> inputKey, String from) {
        List<String> key = aggregate.key() != null ? aggregate.key() : inputKey;
        if (key == null || key.isEmpty()) {
            throw new TapstateException(AggregateError.KEY_REQUIRED, Map.of("step", step, "from", from), null);
        }
        return checked(step, aggregate, input, known, key);
    }

    /**
     * The same checks without settling how an input row is identified, for a caller that knows the
     * input's columns but not its key: the shape of the output does not depend on it.
     */
    public static AggregateStepSpec shape(String step, TransformBody.Aggregate aggregate,
            Map<String, ValueType> input, boolean known) {
        return checked(step, aggregate, input, known, aggregate.key() == null ? List.of() : aggregate.key());
    }

    private static AggregateStepSpec checked(String step, TransformBody.Aggregate aggregate,
            Map<String, ValueType> input, boolean known, List<String> key) {
        if (known) {
            for (String column : key) {
                if (!input.containsKey(column)) {
                    throw invalid(step, "key", "key", "the input has no column " + column);
                }
            }
        }
        RollupScope scope = RollupScope.ofColumns(input, known);
        String where = null;
        if (aggregate.where() != null) {
            where = RollupExpressions.expressionOf(aggregate.where());
            typed(step, "where", "where", where, scope, true);
        }
        Map<String, String> groupBy = new LinkedHashMap<>();
        Map<String, ValueType> groupTypes = new LinkedHashMap<>();
        aggregate.groupBy().forEach((name, written) -> {
            String expression = RollupExpressions.expressionOf(written);
            ValueType type = typed(step, name, "groupBy", expression, scope, false);
            groupBy.put(name, expression);
            groupTypes.put(name, type);
        });
        List<RollupSpec> fields = new ArrayList<>();
        aggregate.fields().forEach((name, field) -> {
            try {
                fields.add(RollupCompiler.compile(name, field, scope));
            } catch (TapstateException refused) {
                throw ofStep(step, refused);
            }
        });
        return new AggregateStepSpec(step, key, where, groupBy, groupTypes, fields);
    }

    private static ValueType typed(String step, String field, String part, String expression,
            RollupScope scope, boolean predicate) {
        RollupExpressions.Typed typed = RollupExpressions.typed(expression, scope, predicate);
        if (typed.error() != null) {
            throw invalid(step, field, part, typed.error());
        }
        return typed.type();
    }

    /** A field's refusal, restated as this step's: the same finding, under the step that owns the field. */
    private static TapstateException ofStep(String step, TapstateException refused) {
        Map<String, Object> args = refused.args();
        if (refused.code() == RollupError.EXPRESSION_INVALID) {
            return new TapstateException(AggregateError.EXPRESSION_INVALID, Map.of("step", step,
                    "field", args.get("field"), "part", args.get("part"), "reason", args.get("reason")), null);
        }
        if (refused.code() == RollupError.TYPE_UNKNOWN) {
            return new TapstateException(AggregateError.TYPE_UNKNOWN, Map.of("step", step,
                    "field", args.get("field"), "expression", args.get("expression")), null);
        }
        return refused;
    }

    private static TapstateException invalid(String step, String field, String part, String reason) {
        int end = reason.indexOf('\n');
        String first = (end < 0 ? reason : reason.substring(0, end)).trim();
        return new TapstateException(AggregateError.EXPRESSION_INVALID,
                Map.of("step", step, "field", field, "part", part, "reason", first), null);
    }

    private static void refuseNameConflicts(String step, TransformBody.Aggregate aggregate) {
        for (String name : aggregate.fields().keySet()) {
            if (aggregate.groupBy().containsKey(name)) {
                throw new TapstateException(AggregateError.NAME_CONFLICT,
                        Map.of("step", step, "name", name), null);
            }
        }
    }

    private static void refuseUnsupportedInput(Step step, Map<String, Step> steps, Map<String, Resource> byId) {
        if (!(step.from() instanceof FromClause.Flow flow) || flow.refs().size() != 1) {
            throw unsupported(step.id(), "(several)", "more than one input; merge them with a union step first");
        }
        refuseBelow(step.id(), flow.refs().getFirst(), steps, byId, new LinkedHashSet<>());
    }

    private static void refuseBelow(String aggregate, FromRef ref, Map<String, Step> steps,
            Map<String, Resource> byId, Set<String> visited) {
        if (!(ref instanceof FromRef.Literal literal)) {
            // A pattern selects source tables, and only source tables.
            return;
        }
        Step upstream = steps.get(literal.ref());
        if (upstream == null || !visited.add(upstream.id())) {
            // Not a step of this pipeline: a source table, which is what the step may read.
            return;
        }
        TransformBody body = bodyOf(upstream, byId);
        if (!(body instanceof TransformBody.Filter || body instanceof TransformBody.MapProjection
                || body instanceof TransformBody.Union)) {
            throw unsupported(aggregate, upstream.id(), body == null ? "unknown" : body.type());
        }
        for (FromRef next : refsOf(upstream.from())) {
            refuseBelow(aggregate, next, steps, byId, visited);
        }
    }

    private static void collectFilters(Step step, Map<String, Step> steps, Map<String, Resource> byId,
            List<String> filters, Set<String> visited) {
        if (step == null) {
            return;
        }
        for (FromRef ref : refsOf(step.from())) {
            if (ref instanceof FromRef.Literal literal && steps.get(literal.ref()) != null
                    && visited.add(literal.ref())) {
                Step upstream = steps.get(literal.ref());
                if (bodyOf(upstream, byId) instanceof TransformBody.Filter) {
                    filters.add(upstream.id());
                }
                collectFilters(upstream, steps, byId, filters, visited);
            }
        }
    }

    private static Collection<FromRef> refsOf(FromClause from) {
        return switch (from) {
            case FromClause.Flow flow -> flow.refs();
            case FromClause.Aliases aliases -> aliases.aliases().values();
        };
    }

    private static TapstateException unsupported(String step, String from, String kind) {
        return new TapstateException(AggregateError.INPUT_NOT_SUPPORTED,
                Map.of("step", step, "from", from, "kind", kind), null);
    }

    /** The body a step runs, taken from the definition it names where it names one. */
    private static TransformBody bodyOf(Step step, Map<String, Resource> byId) {
        return switch (step) {
            case Step.Inline inline -> inline.body();
            case Step.Use use -> byId.get(use.use()) instanceof TransformResource definition
                    ? definition.body()
                    : null;
        };
    }
}
