package io.tapstate.core.lifecycle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Turns a node's cluster-wide target into the per-member width the engine runs it at, for the members
 * taking part in one execution.
 *
 * <p>The engine's own unit is a per-member count: a node runs the same number of processors on every
 * member taking part, so the total it reaches is that count times the number of members, and only the
 * multiples of the member count are reachable. A target that is not one of them lands on the nearest one
 * that every limit allows - of the two per-member counts either side of an exact split, the one whose
 * total is closer to the target, and the smaller of the two when both are as close. A target of one, and a
 * node that can only run as one processor, take the engine's named exception instead: one processor for
 * the whole cluster, which is not a per-member count of one.
 *
 * <p>Nothing here remembers anything. The answer belongs to the members of one execution, and the next
 * execution - after a member left, say - asks again and may get another; carrying an answer over would
 * run a node at a width worked out for a cluster that no longer exists.
 */
public final class ParallelismPlanner {

    private ParallelismPlanner() {
    }

    /** What the planner decided for one node: a width, or a refusal saying why there is none. */
    public sealed interface Decision permits Planned, Refused {
    }

    /** The node runs at this width. */
    public record Planned(NodeParallelism parallelism) implements Decision {
        public Planned {
            Objects.requireNonNull(parallelism, "parallelism");
        }
    }

    /**
     * The node cannot run as asked. {@code singleton} is set where the node can run as one processor only
     * and was explicitly asked for more; otherwise every candidate per-member count broke a limit, and
     * {@code brokenLimits} names the first limit each one broke, in ascending order of candidate.
     */
    public record Refused(
            String node, int requested, int memberCount, ParallelismRequest.Singleton singleton,
            Map<Integer, String> brokenLimits) implements Decision {
        public Refused {
            Objects.requireNonNull(node, "node");
            brokenLimits = brokenLimits == null ? Map.of() : java.util.Collections.unmodifiableMap(
                    new LinkedHashMap<>(brokenLimits));
        }
    }

    /** How wide {@code request} runs on {@code memberCount} members, within {@code budget}. */
    public static Decision plan(ParallelismRequest request, int memberCount, ParallelismBudget budget) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(budget, "budget");
        if (memberCount < 1) {
            throw new IllegalArgumentException("a run takes part on at least one member, got " + memberCount);
        }
        int target = request.target();
        NodeParallelism.Origin origin = request.explicit()
                ? NodeParallelism.Origin.EXPLICIT : NodeParallelism.Origin.NODE_DEFAULT;

        ParallelismRequest.Singleton singleton = request.singleton();
        if (singleton != null && target > 1 && request.explicit() && singleton.refusesExplicit()) {
            return new Refused(request.node(), target, memberCount, singleton, Map.of());
        }
        if (target == 1 || singleton != null) {
            String broken = budget.firstBrokenBy(request, 1);
            if (broken != null) {
                return new Refused(request.node(), target, memberCount, null, Map.of(1, broken));
            }
            List<String> reasons = new ArrayList<>();
            if (target == 1) {
                reasons.add(NodeParallelism.REQUESTED_ONE);
            }
            if (singleton != null) {
                reasons.add(singleton.id());
            }
            return new Planned(new NodeParallelism(request.node(), target, origin,
                    NodeParallelism.Scope.TOTAL_ONE, memberCount, null, 1, reasons));
        }

        TreeSet<Integer> candidates = new TreeSet<>();
        candidates.add(Math.max(1, target / memberCount));
        candidates.add(Math.max(1, ceilDiv(target, memberCount)));

        Map<Integer, String> broken = new LinkedHashMap<>();
        Integer chosen = null;
        for (int local : candidates) {
            String limit = budget.firstBrokenBy(request, local);
            if (limit != null) {
                broken.put(local, limit);
                continue;
            }
            // Ascending order makes the tie go to the smaller total without a second comparison: a later
            // candidate replaces an earlier one only when it is strictly closer to the target.
            if (chosen == null || distance(local, memberCount, target) < distance(chosen, memberCount, target)) {
                chosen = local;
            }
        }
        if (chosen == null) {
            return new Refused(request.node(), target, memberCount, null, broken);
        }

        int effective = chosen * memberCount;
        List<String> reasons = new ArrayList<>();
        if (effective > target) {
            reasons.add(NodeParallelism.ROUNDED_UP);
        } else if (effective < target) {
            reasons.add(NodeParallelism.ROUNDED_DOWN);
        }
        new TreeSet<>(broken.values()).forEach(limit -> reasons.add(NodeParallelism.BUDGET_PREFIX + limit));
        return new Planned(new NodeParallelism(request.node(), target, origin, NodeParallelism.Scope.NATIVE,
                memberCount, chosen, effective, reasons));
    }

    private static long distance(int local, int memberCount, int target) {
        return Math.abs((long) local * memberCount - target);
    }

    private static int ceilDiv(int dividend, int divisor) {
        return -Math.floorDiv(-dividend, divisor);
    }
}
