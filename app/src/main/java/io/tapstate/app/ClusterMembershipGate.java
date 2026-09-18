package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import io.tapstate.spi.store.ClusterMembership;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Non-blocking local view of the durable membership predicate used by data structures and controllers. */
final class ClusterMembershipGate implements SplitBrainProtectionFunction {

    static final String PROTECTION_NAME = "tapstate-committed-membership";
    static final String NODE_ID_ATTRIBUTE = "tapstate.node-id";
    static final String BOOT_ID_ATTRIBUTE = "tapstate.boot-id";
    static final String CONTROL_URL_ATTRIBUTE = "tapstate.control-url";

    private final ClusterProperties.Profile profile;
    private final int bootstrapMinMembers;
    private final AtomicReference<ClusterMembership> committed = new AtomicReference<>();
    private final AtomicReference<Set<String>> visible = new AtomicReference<>(Set.of());

    ClusterMembershipGate(ClusterProperties properties) {
        this.profile = properties.getProfile();
        this.bootstrapMinMembers = properties.getBootstrapMinMembers();
    }

    @Override
    public boolean apply(Collection<Member> members) {
        return eligible(rememberVisible(nodeIds(members)));
    }

    boolean businessEligible() {
        return eligible(visible.get());
    }

    boolean canCommit(Set<String> visibleNodeIds) {
        rememberVisible(visibleNodeIds);
        ClusterMembership current = committed.get();
        if (profile == ClusterProperties.Profile.SINGLE) {
            return true;
        }
        if (current == null) {
            return visibleNodeIds.size() >= bootstrapMinMembers;
        }
        return profile == ClusterProperties.Profile.PRODUCTION_HA
                ? strictMajority(current.activeNodeIds().size(), overlap(current.activeNodeIds(), visibleNodeIds))
                : overlap(current.activeNodeIds(), visibleNodeIds) >= 1;
    }

    void install(ClusterMembership membership) {
        committed.set(membership);
    }

    void failClosed() {
        if (profile != ClusterProperties.Profile.SINGLE) {
            committed.set(null);
        }
    }

    ClusterMembership committed() {
        return committed.get();
    }

    static boolean strictMajority(int committedSize, int visibleCommittedMembers) {
        if (committedSize < 1 || visibleCommittedMembers < 0) {
            throw new IllegalArgumentException("membership counts are out of range");
        }
        return visibleCommittedMembers >= committedSize / 2 + 1;
    }

    static Set<String> nodeIds(Collection<Member> members) {
        Set<String> ids = new HashSet<>();
        for (Member member : members) {
            String nodeId = member.getAttribute(NODE_ID_ATTRIBUTE);
            if (nodeId != null && !nodeId.isBlank()) {
                ids.add(nodeId);
            }
        }
        return Set.copyOf(ids);
    }

    private boolean eligible(Set<String> visibleNodeIds) {
        if (profile == ClusterProperties.Profile.SINGLE) {
            return true;
        }
        ClusterMembership current = committed.get();
        if (current == null) {
            return false;
        }
        int present = overlap(current.activeNodeIds(), visibleNodeIds);
        return profile == ClusterProperties.Profile.PRODUCTION_HA
                ? strictMajority(current.activeNodeIds().size(), present)
                : present >= 1;
    }

    private Set<String> rememberVisible(Set<String> visibleNodeIds) {
        Set<String> snapshot = Set.copyOf(visibleNodeIds);
        visible.set(snapshot);
        return snapshot;
    }

    private static int overlap(Set<String> committed, Set<String> visible) {
        int count = 0;
        for (String node : committed) {
            if (visible.contains(node)) {
                count++;
            }
        }
        return count;
    }
}
