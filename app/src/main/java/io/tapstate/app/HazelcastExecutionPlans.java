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

    @Override
    public void record(ExecutionPlan plan) {
        plans().set(plan.pipelineId(), plan);
        member.<String, ExecutionPlan>getMap(LAST_MAP_NAME).set(plan.pipelineId(), plan);
    }

    @Override
    public ExecutionPlan last(String pipelineId) {
        return member.<String, ExecutionPlan>getMap(LAST_MAP_NAME).get(pipelineId);
    }

    @Override
    public void forget(String pipelineId) {
        plans().delete(pipelineId);
    }

    @Override
    public Map<String, ExecutionPlan> current(Collection<String> pipelineIds) {
        if (pipelineIds.isEmpty()) {
            return Map.of();
        }
        return new LinkedHashMap<>(plans().getAll(new HashSet<>(pipelineIds)));
    }

    private IMap<String, ExecutionPlan> plans() {
        return member.getMap(MAP_NAME);
    }
}
