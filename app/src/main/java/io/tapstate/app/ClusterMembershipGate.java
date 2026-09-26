package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import io.tapstate.spi.store.ClusterMembership;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

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

    /**
     * The data plane's own answer to this same predicate. Until a member binds one, nothing is asked -
     * a build with no member has no data plane to disagree with.
     */
    private final AtomicReference<BooleanSupplier> dataPlane = new AtomicReference<>(() -> true);

    ClusterMembershipGate(ClusterProperties properties) {
        this.profile = properties.getProfile();
        this.bootstrapMinMembers = properties.getBootstrapMinMembers();
    }

    /** A member by the stable id it joined under, or by its engine identity where it names none. */
    static String stableIdOf(Member member) {
        String nodeId = member.getAttribute(NODE_ID_ATTRIBUTE);
        return nodeId != null ? nodeId : member.getUuid().toString();
    }

    /**
     * The members of {@code member}'s cluster a run submitted now would take part on, by stable id and in order:
     * every member that holds data, which in this product is every member - none joins as a lite member.
     */
    static List<String> dataMembers(HazelcastInstance member) {
        return member.getCluster().getMembers().stream()
                .filter(candidate -> !candidate.isLiteMember())
                .map(ClusterMembershipGate::stableIdOf)
                .sorted()
                .toList();
    }

    @Override
    public boolean apply(Collection<Member> members) {
        return eligible(rememberVisible(nodeIds(members)));
    }

    /**
     * Whether this member may take work on, which requires the data plane to agree that it may.
     *
     * <p>Both sides read this one predicate, and they do not read it at the same moment. This side is
     * computed on the spot; the cluster library caches its side and recomputes it when the membership
     * changes and on a timer of its own - so for up to one heartbeat interval after a cluster forms,
     * this side says yes while every ring and map the work would touch still refuses. Work admitted in
     * that window is admitted onto a data plane that will not accept a single write, and what the
     * operator sees is a pipeline that reaches RUNNING and dies, permanently, for a condition that
     * cleared itself seconds later.
     *
     * <p>So admission waits for the slower of the two. It can only ever delay work by the length of
     * that window - the answer this side would have given is still required - and the rest of this
     * class is what makes that answer fail closed. The direction matters: disagreeing by refusing is a
     * pause, and disagreeing by admitting is a dead run.
     */
    boolean businessEligible() {
        return eligible(visible.get()) && dataPlane.get().getAsBoolean();
    }

    /**
     * Binds the data plane's answer, once the member whose answer it is exists.
     *
     * <p>Bound afterwards rather than taken in the constructor because this gate is handed to the
     * member's own configuration: it has to exist before the member it then asks.
     */
    void observeDataPlane(BooleanSupplier admits) {
        dataPlane.set(Objects.requireNonNull(admits, "admits"));
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

    /**
     * The members this one last saw in the cluster, by stable node id, whether committed or not.
     *
     * <p>This is where a member that goes away shows. The committed set only ever grows -- a member that
     * stops answering keeps its place in it, so that the majority it counts toward cannot shrink behind a
     * minority's back -- and so an absence is never visible there. Empty until the first look.
     */
    Set<String> visibleNodeIds() {
        return visible.get();
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
