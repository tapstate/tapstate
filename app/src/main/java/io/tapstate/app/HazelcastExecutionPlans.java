package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ExecutionPlans;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The plans of the runs the cluster is executing, in a map every member shares: written by the member that submits
 * a run, readable from any member a reader asks.
 *
 * <p>Held in the cluster's memory rather than in the store, because a plan belongs to the run it describes and only
 * a run executing now has one worth reading. A cluster restarted from nothing has no run executing, and its next
 * run writes a plan of its own; a member leaving loses nothing, since the map keeps a copy on another member.
 */
final class HazelcastExecutionPlans implements ExecutionPlans, ExecutionPlanRecorder {

    /** The map the plans are kept in, one entry per pipeline with a run. */
    static final String MAP_NAME = "tapstate.execution-plans";

    /**
     * The map the latest plan of each pipeline that ever ran is kept in, whether or not its run is still going:
     * what the next run's plan is compared against, across a stop as much as across a lost member.
     */
    static final String LAST_MAP_NAME = "tapstate.execution-plans.last";

    private final HazelcastInstance member;

    HazelcastExecutionPlans(HazelcastInstance member) {
        this.member = Objects.requireNonNull(member, "member");
    }

    private record ScopedPlan(ExecutionFence original, ExecutionPlan plan, boolean live) implements java.io.Serializable {}

    /** Late record/forget callbacks can change only their original execution's derived plan slot. */
    ExecutionPlanRecorder forRun(ExecutionFence original, Runnable active) {
        Objects.requireNonNull(original); Objects.requireNonNull(active);
        if (original.scope() == null) throw ThreeLayerStorePort.refused();
        return new ExecutionPlanRecorder() {
            @Override public void record(ExecutionPlan plan) {
                if (!original.pipelineId().equals(plan.pipelineId())
                        || !Long.valueOf(original.claimGeneration()).equals(plan.claimGeneration())
                        || !Long.valueOf(original.executionGeneration()).equals(plan.executionGeneration())
                        || !original.topologyRevision().equals(plan.topologyRevision())) throw ThreeLayerStorePort.refused();
                publish(LAST_MAP_NAME, new ScopedPlan(original, plan, true), active);
                publish(MAP_NAME, new ScopedPlan(original, plan, true), active);
            }
            @Override public void forget(String pipeline) {
                // Confirmed retirement may finish after authorization expired; exact CAS cannot remove a successor.
                if (!original.pipelineId().equals(pipeline)) throw ThreeLayerStorePort.refused();
                var map = member.<String,Object>getMap(MAP_NAME); Object held = map.get(pipeline);
                if (held == null) return;
                if (!(held instanceof ScopedPlan prior)) throw ThreeLayerStorePort.refused();
                if (prior.original().equals(original) && prior.live())
                    map.replace(pipeline, prior, new ScopedPlan(original, prior.plan(), false));
            }
            @Override public void forget(ExecutionPlan expected) {
                Objects.requireNonNull(expected, "expected");
                if (!original.pipelineId().equals(expected.pipelineId())
                        || !Long.valueOf(original.claimGeneration()).equals(expected.claimGeneration())
                        || !Long.valueOf(original.executionGeneration()).equals(expected.executionGeneration())
                        || !original.topologyRevision().equals(expected.topologyRevision())) throw ThreeLayerStorePort.refused();
                var map = member.<String,Object>getMap(MAP_NAME); Object held = map.get(expected.pipelineId());
                if (held == null) return;
                if (!(held instanceof ScopedPlan prior)) throw ThreeLayerStorePort.refused();
                if (prior.original().equals(original) && prior.live() && prior.plan().equals(expected))
                    map.replace(expected.pipelineId(), prior, new ScopedPlan(original, expected, false));
            }
            @Override public ExecutionPlan last(String pipeline) {
                active.run(); if (!original.pipelineId().equals(pipeline)) throw ThreeLayerStorePort.refused();
                Object held = member.<String,Object>getMap(LAST_MAP_NAME).get(pipeline); active.run();
                if (held == null) return null;
                if (!(held instanceof ScopedPlan prior) || !sameBusiness(prior.original(), original)) throw ThreeLayerStorePort.refused();
                return prior.plan();
            }
        };
    }

    private void publish(String name, ScopedPlan candidate, Runnable active) {
        var map = member.<String,Object>getMap(name); String pipeline = candidate.original().pipelineId();
        for (int attempt = 0; attempt < 32; attempt++) {
            active.run(); Object held = map.get(pipeline); active.run();
            if (held == null) { if (map.putIfAbsent(pipeline, candidate) == null) return; }
            else {
                if (!(held instanceof ScopedPlan prior) || !sameBusiness(prior.original(), candidate.original())
                        || prior.original().scope().stateGeneration() > candidate.original().scope().stateGeneration()
                        || prior.original().claimGeneration() > candidate.original().claimGeneration()
                        || prior.original().executionGeneration() > candidate.original().executionGeneration()) throw ThreeLayerStorePort.refused();
                if (prior.original().equals(candidate.original()) && !prior.plan().equals(candidate.plan())) throw ThreeLayerStorePort.refused();
                if (prior.original().equals(candidate.original()) && !prior.live() && candidate.live()) throw ThreeLayerStorePort.refused();
                if (map.replace(pipeline, held, candidate)) return;
            }
        }
        throw ThreeLayerStorePort.refused();
    }
    private static boolean sameBusiness(ExecutionFence left, ExecutionFence right) {
        return left.scope() != null && right.scope() != null && left.pipelineId().equals(right.pipelineId())
                && left.scope().clusterId().equals(right.scope().clusterId())
                && left.scope().bindingVersion() == right.scope().bindingVersion()
                && left.scope().cacheIncarnation().equals(right.scope().cacheIncarnation());
    }
    private static ExecutionPlan value(Object held) {
        if (held == null) return null;
        if (held instanceof ScopedPlan scoped) return scoped.plan();
        if (held instanceof ExecutionPlan plan) return plan;
        throw ThreeLayerStorePort.refused();
    }

    @Override
    public void record(ExecutionPlan plan) {
        if (member.<String,Object>getMap(MAP_NAME).get(plan.pipelineId()) instanceof ScopedPlan
                || member.<String,Object>getMap(LAST_MAP_NAME).get(plan.pipelineId()) instanceof ScopedPlan) throw ThreeLayerStorePort.refused();
        plans().set(plan.pipelineId(), plan);
        member.<String, ExecutionPlan>getMap(LAST_MAP_NAME).set(plan.pipelineId(), plan);
    }

    @Override
    public ExecutionPlan last(String pipelineId) {
        return value(member.<String, Object>getMap(LAST_MAP_NAME).get(pipelineId));
    }

    @Override
    public void forget(String pipelineId) {
        if (member.<String,Object>getMap(MAP_NAME).get(pipelineId) instanceof ScopedPlan) throw ThreeLayerStorePort.refused();
        plans().delete(pipelineId);
    }

    @Override
    public void forget(ExecutionPlan expected) {
        plans().remove(expected.pipelineId(), expected);
    }

    @Override
    public Map<String, ExecutionPlan> current(Collection<String> pipelineIds) {
        if (pipelineIds.isEmpty()) {
            return Map.of();
        }
        try {
            var result = new LinkedHashMap<String,ExecutionPlan>();
            member.<String,Object>getMap(MAP_NAME).getAll(new HashSet<>(pipelineIds)).forEach((id, held) -> {
                if (!(held instanceof ScopedPlan scoped) || scoped.live()) result.put(id, value(held));
            });
            return result;
        } catch (RuntimeException failed) {
            if (!HazelcastLivePipelineRuns.theClusterIsChanging(failed)) {
                throw failed;
            }
            // Asked while the engine is down, or while the cluster changes under the read: a plan only a running
            // engine holds is not known here now. The read faces say so by leaving it out, not by failing the part
            // of their answer the store holds along with it. Writing a plan is not let off - the start writing one
            // has no engine to submit to either.
            return Map.of();
        }
    }

    private IMap<String, ExecutionPlan> plans() {
        return member.getMap(MAP_NAME);
    }
}
