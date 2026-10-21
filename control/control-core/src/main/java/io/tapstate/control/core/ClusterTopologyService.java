package io.tapstate.control.core;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNodeReading;
import io.tapstate.spi.store.ClusterNodeRegistration;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.WorkloadClaim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Joins what the engine sees right now with what the cluster has committed, and answers with the pair.
 *
 * <p>Neither half is the topology on its own. The engine's member list says who is reachable, which
 * includes a node that has just arrived and has not been committed by anybody; the committed set says
 * who is entitled to do work, which includes a node that has gone away and whose removal has not been
 * committed yet. Reporting either alone is reporting a cluster that does not exist, and the difference
 * between them is precisely what a reader is looking for when something is wrong.
 *
 * <p>The stable registry keeps departed nodes visible without pretending they are live members. Join
 * evidence belongs to one boot: a reserved boot not yet seen joining is different from a boot which
 * joined and disappeared. Session leases are read on the store clock, never judged on this node's clock.
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
    private final ClusterProfileStore profiles;
    private final ClusterRecoveryQueries recovery;

    /**
     * @param committed the committed-membership store, or null on a build that keeps no membership --
     *                  a single node is the whole cluster, and there is nothing for it to be outside of
     */
    public ClusterTopologyService(
            LiveClusterMembers live,
            ClusterMembershipStore committed,
            ClusterPipelineTopologyService pipelines,
            String clusterId) {
        this(live, committed, pipelines, clusterId, null);
    }

    /** Reads admitted profiles and node sessions alongside live membership. */
    public ClusterTopologyService(LiveClusterMembers live, ClusterMembershipStore committed,
            ClusterPipelineTopologyService pipelines, String clusterId, ClusterProfileStore profiles) {
        this(live, committed, pipelines, clusterId, profiles, ClusterRecoveryQueries.NONE);
    }

    public ClusterTopologyService(LiveClusterMembers live, ClusterMembershipStore committed,
            ClusterPipelineTopologyService pipelines, String clusterId, ClusterProfileStore profiles,
            ClusterRecoveryQueries recovery) {
        this.live = Objects.requireNonNull(live, "live");
        this.committed = committed;
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
        this.clusterId = clusterId;
        this.profiles = profiles;
        this.recovery = Objects.requireNonNull(recovery, "recovery");
    }

    /** The cluster as the answering node can see it right now. */
    public ClusterTopologyView topology() {
        Optional<ClusterMembership> membership = committed == null || clusterId == null
                ? Optional.empty()
                : committed.read(clusterId);
        // Absent membership is not an empty membership: with nothing committed there is no set to be
        // outside of, so nobody is reported as still joining one.
        Set<String> active = membership.map(ClusterMembership::activeNodeIds).orElse(null);
        ClusterExecutionProfile profile = profiles == null || clusterId == null
                ? null : profiles.profile(clusterId).orElse(null);
        Map<String, ClusterNodeReading> registered = new LinkedHashMap<>();
        if (profiles != null && clusterId != null) {
            for (ClusterNodeReading reading : profiles.nodes(clusterId)) {
                registered.put(reading.registration().nodeSession().owner().nodeId(), reading);
            }
        }
        List<ClusterMemberView> members = new ArrayList<>();
        for (LiveClusterMember member : live.members()) {
            ClusterNodeReading reading = registered.remove(member.nodeId());
            members.add(member(member, reading, active, profile));
        }
        for (ClusterNodeReading reading : registered.values()) {
            members.add(member(null, reading, active, profile));
        }
        members.sort(Comparator.comparing(
                ClusterMemberView::nodeId, Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparing(ClusterMemberView::bootId, Comparator.nullsLast(Comparator.naturalOrder())));
        return new ClusterTopologyView(
                clusterId,
                membership.map(ClusterMembership::revision).orElse(null),
                members,
                // The member half goes into the pipeline half: a processor is reported under the
                // engine's identity for this run of a member while a claim names the stable one, and
                // which members a run is carrying no part of is a question about both.
                pipelines.pipelines(members),
                profile == null ? null : profile.generation(),
                profile == null ? null : profile.profile().hash(), recovery.cluster());
    }

    private ClusterMemberView member(LiveClusterMember member, ClusterNodeReading reading,
            Set<String> active, ClusterExecutionProfile profile) {
        if (reading == null) {
            return new ClusterMemberView(member.nodeId(), member.memberUuid(), member.bootId(),
                    member.hzAddress(), member.controlUrl(),
                    profiles == null ? stateOf(member, active) : ClusterMemberState.INCOMPATIBLE,
                    null, null, null, null, null, null, null, null, true, null, null);
        }
        ClusterNodeRegistration node = reading.registration();
        WorkloadClaim session = node.nodeSession();
        ClusterMemberState state;
        if (member == null) {
            state = reading.leased() && !node.joined()
                    ? ClusterMemberState.JOINING : ClusterMemberState.LOST;
        } else if (profile == null
                || node.profile().generation() != profile.generation()
                || !node.profile().profile().hash().equals(profile.profile().hash())
                || !session.owner().bootId().equals(member.bootId())
                || (node.joined() && !Objects.equals(node.memberUuid(), member.memberUuid()))) {
            state = ClusterMemberState.INCOMPATIBLE;
        } else if (!reading.leased()) {
            state = ClusterMemberState.LOST;
        } else {
            state = active != null && active.contains(session.owner().nodeId())
                    ? ClusterMemberState.ACTIVE : ClusterMemberState.JOINING;
        }
        return new ClusterMemberView(session.owner().nodeId(),
                member == null ? node.memberUuid() : member.memberUuid(),
                member == null ? session.owner().bootId() : member.bootId(),
                member == null ? node.memberAddress() : member.hzAddress(),
                member == null ? node.controlUrl().toString() : member.controlUrl(), state,
                node.profile().generation(), node.profile().profile().hash(), session.owner().bootId(),
                session.leaseUntil(), reading.leaseRemaining().toMillis(), reading.leased(),
                node.joined(), node.joinedAt(), member != null, node.memberUuid(), node.memberAddress());
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
