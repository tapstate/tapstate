package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.NodeParallelism;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * How wide each pipeline node of one execution runs, worked out before the graph is drawn, and what each
 * node that runs wider than one processor routes its input by.
 *
 * <p>{@code plannedMembers} is the member count the widths were worked out for. A node drawn from here
 * checks it against the members its execution actually starts on, because a run that started on a different
 * number of members would run a different number of processors than was worked out - and anything counting
 * on the worked-out number, such as the set of writers whose progress has to be waited for, would be wrong
 * about it.
 *
 * <p>{@code inputKeys} names, for each node that runs wider than one processor, the key columns of every
 * stream reaching it: the same key must reach the same processor, and which columns make up a key is a
 * property of the stream at that point in the graph, not of the node.
 *
 * <p>{@code sinkTargets} names, for each sink that runs wider than one processor, the target table and key of
 * every stream reaching it. A sink routes by where a row lands rather than by the stream it came in on, which
 * is what lets two streams written into one target table keep a key's changes on one writer.
 *
 * <p>A node this shape says nothing about runs the way every node ran before shapes existed: one processor
 * for the whole cluster.
 */
public record ExecutionShape(
        int plannedMembers,
        Map<String, NodeParallelism> nodes,
        Map<String, Map<String, List<String>>> inputKeys,
        Map<String, Map<String, SinkTarget>> sinkTargets) {

    public ExecutionShape {
        if (plannedMembers < 1) {
            throw new IllegalArgumentException("a run is planned over at least one member, got " + plannedMembers);
        }
        nodes = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(nodes, "nodes")));
        Map<String, Map<String, List<String>>> keys = new LinkedHashMap<>();
        Objects.requireNonNull(inputKeys, "inputKeys").forEach((node, byStream) -> {
            Map<String, List<String>> copy = new LinkedHashMap<>();
            byStream.forEach((stream, columns) -> copy.put(stream, List.copyOf(columns)));
            keys.put(node, Collections.unmodifiableMap(copy));
        });
        inputKeys = Collections.unmodifiableMap(keys);
        Map<String, Map<String, SinkTarget>> targets = new LinkedHashMap<>();
        Objects.requireNonNull(sinkTargets, "sinkTargets").forEach((node, byStream) ->
                targets.put(node, Collections.unmodifiableMap(new LinkedHashMap<>(byStream))));
        sinkTargets = Collections.unmodifiableMap(targets);
        nodes.forEach((node, parallelism) -> {
            if (parallelism.scope() == NodeParallelism.Scope.NATIVE
                    && parallelism.memberCount() != plannedMembers) {
                throw new IllegalArgumentException("node '" + node + "' was worked out for "
                        + parallelism.memberCount() + " members, the run for " + plannedMembers);
            }
        });
    }

    /** A shape in which no sink runs wider than one processor. */
    public ExecutionShape(int plannedMembers, Map<String, NodeParallelism> nodes,
            Map<String, Map<String, List<String>>> inputKeys) {
        this(plannedMembers, nodes, inputKeys, Map.of());
    }

    /** A shape that runs every node as one processor for the cluster: how every graph ran before shapes. */
    public static ExecutionShape totalOne() {
        return new ExecutionShape(1, Map.of(), Map.of());
    }

    /**
     * The target table and key of each stream reaching the sink {@code node}; only asked of a sink that runs
     * natively.
     */
    public Map<String, SinkTarget> sinkTargetsOf(String node) {
        Map<String, SinkTarget> targets = sinkTargets.get(node);
        if (targets == null) {
            throw new IllegalStateException("sink '" + node + "' runs natively but no targets were worked out");
        }
        return targets;
    }

    /** How many processors {@code node} runs across the cluster: its per-member count times the members. */
    public int effectiveOf(String node) {
        return localOf(node) * plannedMembers;
    }

    /** Whether {@code node} runs the same number of processors on every member rather than one in total. */
    public boolean isNative(String node) {
        NodeParallelism parallelism = nodes.get(node);
        return parallelism != null && parallelism.scope() == NodeParallelism.Scope.NATIVE;
    }

    /** The processors {@code node} runs on each member; only asked of a node that {@link #isNative runs natively}. */
    public int localOf(String node) {
        NodeParallelism parallelism = nodes.get(node);
        if (parallelism == null || parallelism.scope() != NodeParallelism.Scope.NATIVE) {
            throw new IllegalStateException("node '" + node + "' does not run natively in this shape");
        }
        return parallelism.computedLocal();
    }

    /** The key columns of each stream reaching {@code node}; only asked of a node that runs natively. */
    public Map<String, List<String>> inputKeysOf(String node) {
        Map<String, List<String>> keys = inputKeys.get(node);
        if (keys == null) {
            throw new IllegalStateException("node '" + node + "' runs natively but no input keys were worked out");
        }
        return keys;
    }

    /** Whether any node runs natively, which is what decides how a sink below it reads its input. */
    public boolean anyNative() {
        return nodes.values().stream().anyMatch(parallelism -> parallelism.scope() == NodeParallelism.Scope.NATIVE);
    }
}
