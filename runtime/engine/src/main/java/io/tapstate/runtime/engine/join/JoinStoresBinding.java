package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstance;

import java.io.Serializable;
import java.util.Objects;

/**
 * What the assembly root supplies for a join node: where its state lives.
 *
 * <p>The engine deliberately does not decide that. Choosing a store would mean choosing between a heap
 * and a disk, and between one member and a cluster; the graph is the same either way.
 *
 * <p>Carried to the member that runs the vertex, which is why it is serializable: what travels is the
 * instruction for building the state, never a state object built here.
 */
@FunctionalInterface
public interface JoinStoresBinding extends Serializable {

    /** The state one join step keeps, on the member this vertex is running on. */
    JoinStores bind(HazelcastInstance member, String pipelineId, String stepId);

    /**
     * The state on the cluster: three distributed maps reading through to the cold layer behind them.
     * What every deployment runs, and the only binding whose state outlives the member holding it.
     */
    static JoinStoresBinding onTheCluster() {
        return (member, pipelineId, stepId) -> {
            Objects.requireNonNull(member, "member");
            return new ImapJoinStores(member, pipelineId, stepId);
        };
    }
}
