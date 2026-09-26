package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.core.lifecycle.ParallelismPlanner;
import io.tapstate.core.lifecycle.ParallelismRequest;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.SinkTarget;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * Works out how wide each node of one run is, for the members that run takes part on, and what each node
 * that runs wider than one processor routes its input by.
 *
 * <p>The widths come from the planner; what this adds is what the planner cannot see from a target alone -
 * whether the rows reaching a node carry a key at all. A key is a property of a stream at a point in the
 * graph: a source table's key travels through a filter or a script unchanged, a projection can rename a key
 * column or take it away, and an unwind's rows are no longer the table's rows. So the walk carries, from each
 * producer, the key columns of every stream it emits, and a node whose input has a stream without one can only
 * run as one processor - refused where its author explicitly asked for more.
 *
 * <p>A sink routes by where its rows land rather than by the streams they arrive on: each stream reaching it is
 * written into a target table, and a row goes to the writer its key in that table belongs to. So a sink can run
 * wider than one processor unless every row it writes lands in one table that has no key, or a stream reaches
 * it that nothing says the landing of. A sink writing several tables is not held to one by a keyless table
 * among them: that table's rows all go to one writer and only that table is written serially.
 *
 * <p>The stateless steps, the unions and the sinks are worked out here. Every other node runs the way it ran
 * before a run had a shape.
 */
final class ExecutionShapes {

    private ExecutionShapes() {
    }

    /**
     * One sink a run draws: the vertex it is drawn as, the execution block its author wrote, if any, and the
     * table each stream reaching it lands in, with that table's key there - the same tables its writers are
     * handed. A stream nothing says the landing of maps to null.
     */
    record Sink(String vertex, ExecutionSpec execution, Map<String, SinkTarget> targets) {

        Sink {
            Objects.requireNonNull(vertex, "vertex");
            targets = Collections.unmodifiableMap(new LinkedHashMap<>(targets));
        }
    }

    /**
     * What the walk needs to know about the graph: the producers a reference names, the stream each source
     * vertex emits, the key columns of each source table, and the key columns of what each assembling step -
     * a nest or a join - emits. A table or step with no key maps to an empty list.
     */
    record Graph(
            Function<FromRef, List<String>> upstreams,
            Map<String, String> streamOfSourceVertex,
            Map<String, List<String>> tableKeys,
            Map<String, List<String>> assembledKeys) {

        Graph {
            Objects.requireNonNull(upstreams, "upstreams");
            streamOfSourceVertex = Map.copyOf(streamOfSourceVertex);
            tableKeys = Map.copyOf(tableKeys);
            assembledKeys = Map.copyOf(assembledKeys);
        }
    }

    /** The shape of {@code pipeline}'s run on {@code members} members within {@code budget}. */
    static ExecutionShape of(String pipelineId, PipelineResource pipeline, int members, ParallelismBudget budget,
            Graph graph, List<Sink> sinks) {
        Map<String, Map<String, List<String>>> emitted = new LinkedHashMap<>();
        graph.streamOfSourceVertex().forEach((vertex, stream) ->
                emitted.put(vertex, Map.of(stream, graph.tableKeys().getOrDefault(stream, List.of()))));

        Map<String, NodeParallelism> nodes = new LinkedHashMap<>();
        Map<String, Map<String, List<String>>> inputKeys = new LinkedHashMap<>();
        for (Step step : pipeline.transforms() == null ? List.<Step>of() : pipeline.transforms()) {
            if (!(step instanceof Step.Inline inline)) {
                continue;
            }
            TransformBody body = inline.body();
            if (body instanceof TransformBody.Nest || body instanceof TransformBody.Join) {
                emitted.put(step.id(), Map.of(step.id(), graph.assembledKeys().getOrDefault(step.id(), List.of())));
                continue;
            }
            Map<String, List<String>> input = inputOf(inline.from(), graph, emitted);
            emitted.put(step.id(), switch (body) {
                case TransformBody.MapProjection map -> projected(input, map.fields());
                case TransformBody.Unwind ignored -> withoutKeys(input);
                default -> input;
            });

            ExecutionSpec execution = step.execution();
            boolean keyed = !input.isEmpty() && input.values().stream().noneMatch(List::isEmpty);
            ParallelismRequest request = new ParallelismRequest(step.id(), ParallelismRequest.Kind.TRANSFORM,
                    writtenIn(execution), keyed ? null : ParallelismRequest.Singleton.KEY_NOT_DERIVABLE, false,
                    batchOf(execution).effectiveMaxRecords(), 0);
            NodeParallelism parallelism = planned(pipelineId, ParallelismPlanner.plan(request, members, budget));
            nodes.put(step.id(), parallelism);
            if (parallelism.scope() == NodeParallelism.Scope.NATIVE) {
                inputKeys.put(step.id(), input);
            }
        }

        Map<String, Map<String, SinkTarget>> sinkTargets = new LinkedHashMap<>();
        for (Sink sink : sinks) {
            // Each writer opens a connector of its own: an artifact certified to share one is only known on
            // the member that resolves it, so the budget is worked out for the instances that could open.
            ParallelismRequest request = new ParallelismRequest(sink.vertex(), ParallelismRequest.Kind.SINK,
                    writtenIn(sink.execution()), singletonOf(sink.targets()), false,
                    batchOf(sink.execution()).effectiveMaxRecords(), 0);
            NodeParallelism parallelism = planned(pipelineId, ParallelismPlanner.plan(request, members, budget));
            nodes.put(sink.vertex(), parallelism);
            if (parallelism.scope() == NodeParallelism.Scope.NATIVE) {
                sinkTargets.put(sink.vertex(), sink.targets());
            }
        }
        return new ExecutionShape(members, nodes, inputKeys, sinkTargets);
    }

    /**
     * Why a sink can run as one processor only, or null where it can run wider: a stream reaches it that nothing
     * says the landing of, or every row it writes lands in one table with no key to tell its rows apart by.
     */
    private static ParallelismRequest.Singleton singletonOf(Map<String, SinkTarget> targets) {
        if (targets.isEmpty() || targets.containsValue(null)) {
            return ParallelismRequest.Singleton.KEY_NOT_DERIVABLE;
        }
        Set<String> tables = new HashSet<>();
        targets.values().forEach(target -> tables.add(target.table()));
        boolean anyKeyless = targets.values().stream().anyMatch(target -> !target.keyed());
        return tables.size() == 1 && anyKeyless ? ParallelismRequest.Singleton.SINGLE_TARGET_KEYLESS : null;
    }

    private static Integer writtenIn(ExecutionSpec execution) {
        return execution == null ? null : execution.parallelism();
    }

    private static BatchSpec batchOf(ExecutionSpec execution) {
        return execution == null ? BatchSpec.DEFAULTS : execution.batchOrDefaults();
    }

    /** Every stream reaching a step from its {@code from:} list, with the key each carries there. */
    private static Map<String, List<String>> inputOf(
            FromClause from, Graph graph, Map<String, Map<String, List<String>>> emitted) {
        Map<String, List<String>> input = new LinkedHashMap<>();
        if (from instanceof FromClause.Flow flow) {
            for (FromRef ref : flow.refs()) {
                for (String producer : graph.upstreams().apply(ref)) {
                    Map<String, List<String>> streams = emitted.get(producer);
                    if (streams == null) {
                        // A producer the walk has not reached: the builder refuses the same graph for the same
                        // reason, so nothing here is guessed on its behalf.
                        throw new IllegalStateException("reference " + ref + " names '" + producer
                                + "', which nothing before it produces");
                    }
                    streams.forEach(input::putIfAbsent);
                }
            }
        }
        return input;
    }

    /**
     * The keys a projection leaves its streams with. A key column survives unchanged unless a rule touches
     * it: dropped, or written over by a value, a computation or another column, the stream is left with no
     * key; moved to a new name by a rename that consumes it, the key follows it there.
     */
    private static Map<String, List<String>> projected(Map<String, List<String>> input, Map<String, FieldRule> fields) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        input.forEach((stream, key) -> out.put(stream, projectedKey(key, fields)));
        return out;
    }

    private static List<String> projectedKey(List<String> key, Map<String, FieldRule> fields) {
        List<String> projected = new ArrayList<>(key.size());
        for (String column : key) {
            FieldRule own = fields.get(column);
            if (own != null && !(own instanceof FieldRule.Rename rename && rename.sourceField().equals(column))) {
                return List.of();
            }
            String movedTo = null;
            for (Map.Entry<String, FieldRule> rule : fields.entrySet()) {
                if (rule.getValue() instanceof FieldRule.Rename rename && rename.sourceField().equals(column)
                        && !rule.getKey().equals(column)) {
                    movedTo = rule.getKey();
                    break;
                }
            }
            projected.add(movedTo != null ? movedTo : column);
        }
        return List.copyOf(projected);
    }

    /** The same streams with no key: an unwind's rows are elements, not the rows the key identified. */
    private static Map<String, List<String>> withoutKeys(Map<String, List<String>> input) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        input.keySet().forEach(stream -> out.put(stream, List.of()));
        return out;
    }

    private static NodeParallelism planned(String pipelineId, ParallelismPlanner.Decision decision) {
        return switch (decision) {
            case ParallelismPlanner.Planned planned -> planned.parallelism();
            case ParallelismPlanner.Refused refused when refused.singleton() != null ->
                    throw new TapstateException(ActuationError.PARALLELISM_NEEDS_A_KEY, Map.of(
                            "pipeline", pipelineId, "node", refused.node(), "requested", refused.requested(),
                            "reason", refused.singleton().id()), null);
            case ParallelismPlanner.Refused refused -> {
                StringJoiner candidates = new StringJoiner("; ");
                refused.brokenLimits().forEach((local, limit) ->
                        candidates.add(local + " per member breaks " + limit));
                throw new TapstateException(ActuationError.NO_SAFE_PARALLELISM, Map.of(
                        "pipeline", pipelineId, "node", refused.node(), "requested", refused.requested(),
                        "members", refused.memberCount(), "candidates", candidates.toString()), null);
            }
        };
    }
}
