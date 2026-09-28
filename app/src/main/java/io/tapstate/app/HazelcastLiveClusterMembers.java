package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import io.tapstate.control.core.ClusterError;
import io.tapstate.control.core.LiveClusterMember;
import io.tapstate.control.core.LiveClusterMembers;

import io.tapstate.core.common.TapstateException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The engine's own member list, read the only way it can be read: by asking the member this process is.
 *
 * <p>Each member carries its Tapstate identity as engine member attributes, set once before it joins.
 * They travel with membership itself, so every member holds every other's without a round trip and
 * without a second place for them to be stale in -- which is what makes this answer the same on every
 * node, and what keeps the durable half (the node registry, the claims) from having to be consulted for
 * something that is true only while a process is up.
 *
 * <p>A member that carries none of them answers null for each: a member of this cluster that is not one
 * of ours is a thing worth reporting as it is, not one to invent an identity for.
 */
final class HazelcastLiveClusterMembers implements LiveClusterMembers {

    private final HazelcastInstance member;

    HazelcastLiveClusterMembers(HazelcastInstance member) {
        this.member = Objects.requireNonNull(member, "member");
    }

    @Override
    public List<LiveClusterMember> members() {
        List<LiveClusterMember> live = new ArrayList<>();
        try {
            for (Member peer : member.getCluster().getMembers()) {
                live.add(new LiveClusterMember(
                        peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),
                        peer.getUuid() == null ? null : peer.getUuid().toString(),
                        peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE),
                        peer.getAddress() == null ? null : peer.getAddress().toString(),
                        peer.getAttribute(ClusterMembershipGate.CONTROL_URL_ATTRIBUTE)));
            }
        } catch (HazelcastInstanceNotActiveException notActive) {
            // Not a failure of this process: the member this one is shuts its instance down when it can
            // no longer prove its node session, and the library restarts it on the smaller side once a
            // split brain heals -- while this face, being neither, goes on being asked. Refusing with a
            // code is what keeps those windows apart from an answer; left to throw, it is an uncoded
            // page, which a caller cannot tell from the product having fallen over.
            //
            // Only this one exception, and only around the asking: anything else coming out of here is a
            // defect, and a defect filed under a code that says the member is merely busy is a defect
            // nobody goes looking for.
            throw new TapstateException(ClusterError.MEMBERSHIP_UNREADABLE, Map.of(), notActive);
        }
        return List.copyOf(live);
    }
}
