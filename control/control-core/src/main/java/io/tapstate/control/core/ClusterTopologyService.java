package io.tapstate.control.core;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Joins what the engine sees right now with what the cluster has committed, and answers with the pair.
 *
 * <p>Neither half is the topology on its own. The engine's member list says who is reachable, which
 * includes a node that has just arrived and has not been committed by anybody; the committed set says
 * who is entitled to do work, which includes a node that has gone away and whose removal has not been
 * committed yet. Reporting either alone is reporting a cluster that does not exist, and the difference
 * between them is precisely what a reader is looking for when something is wrong.
 *
 * <p>A member the committed set names but the engine cannot see is deliberately not listed: this
 * answers what is here, and a node that is gone is absent from it. That it was committed is readable
 * from the claims it still holds, which outlive it by exactly one lease.
 *
 * <p>The pipeline half is joined on here rather than asked for separately, because the two halves answer
 * one question between them: a pipeline's work is reported against members, and a reader given the
 * pipelines without the members they run on would have to take a second reading to make sense of the
 * first -- by which time the cluster may have changed underneath both.
 */
public final class ClusterTopologyService {

    private final LiveClusterMembers live;
    private final ClusterMembershipStore committed;
    private final ClusterPipelineTopologyService pipelines;
    private final String clusterId;

    /**
     * @param committed the committed-membership store, or null on a build that keeps no membership --
     *                  a single node is the whole cluster, and there is nothing for it to be outside of
     */
    public ClusterTopologyService(
            LiveClusterMembers live,
            ClusterMembershipStore committed,
            ClusterPipelineTopologyService pipelines,
            String clusterId) {
        this.live = Objects.requireNonNull(live, "live");
        this.committed = committed;
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
        this.clusterId = clusterId;
    }

    /** The cluster as the answering node can see it right now. */
    public ClusterTopologyView topology() {
        Optional<ClusterMembership> membership = committed == null || clusterId == null
                ? Optional.empty()
                : committed.read(clusterId);
        // Absent membership is not an empty membership: with nothing committed there is no set to be
        // outside of, so nobody is reported as still joining one.
        Set<String> active = membership.map(ClusterMembership::activeNodeIds).orElse(null);
        List<ClusterMemberView> members = new ArrayList<>();
        for (LiveClusterMember member : live.members()) {
            members.add(new ClusterMemberView(
                    member.nodeId(), member.memberUuid(), member.bootId(),
                    member.hzAddress(), member.controlUrl(), stateOf(member, active)));
        }
        members.sort(Comparator.comparing(
                ClusterMemberView::nodeId, Comparator.nullsLast(Comparator.naturalOrder())));
        return new ClusterTopologyView(
                clusterId,
                membership.map(ClusterMembership::revision).orElse(null),
                members,
                // The member half goes into the pipeline half: a processor is reported under the
                // engine's identity for this run of a member while a claim names the stable one, and
                // which members a run is carrying no part of is a question about both.
                pipelines.pipelines(members));
    }

    private static ClusterMemberState stateOf(LiveClusterMember member, Set<String> active) {
        if (active == null) {
            return ClusterMemberState.ACTIVE;
        }
        return member.nodeId() != null && active.contains(member.nodeId())
                ? ClusterMemberState.ACTIVE
                : ClusterMemberState.JOINING;
    }
}
