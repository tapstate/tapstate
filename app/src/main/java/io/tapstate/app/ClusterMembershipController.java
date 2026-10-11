package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNativeMember;
import io.tapstate.spi.store.ClusterNativeObservation;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Commits bootstrap and join topology revisions only from a view qualified by the prior revision.
 *
 * <p>Departures are not committed here at all, and {@link #joinsOnly} says why: this loop cannot tell a
 * member that died from one behind a cut, and the set it would write is the denominator of the majority
 * that decides who may act.
 */
final class ClusterMembershipController implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterMembershipController.class);

    private final String clusterId;
    private final HazelcastInstance member;
    private final ClusterMembershipStore store;
    private final ClusterMembershipGate gate;
    private final WorkloadClaim nodeSession;
    private final ClusterProfileStore profiles;
    private final Duration nativeViewTtl;
    private final ScheduledExecutorService reconciler;
    private final AtomicBoolean closed = new AtomicBoolean();

    ClusterMembershipController(
            String clusterId,
            HazelcastInstance member,
            ClusterMembershipStore store,
            ClusterMembershipGate gate,
            Duration interval) {
        this(clusterId, member, store, gate, interval, null);
    }

    ClusterMembershipController(String clusterId, HazelcastInstance member, ClusterMembershipStore store,
            ClusterMembershipGate gate, Duration interval, WorkloadClaim nodeSession) {
        this(clusterId, member, store, gate, interval, nodeSession, null);
    }

    ClusterMembershipController(String clusterId, HazelcastInstance member, ClusterMembershipStore store,
            ClusterMembershipGate gate, Duration interval, WorkloadClaim nodeSession, ClusterProfileStore profiles) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.member = Objects.requireNonNull(member, "member");
        this.store = Objects.requireNonNull(store, "store");
        this.gate = Objects.requireNonNull(gate, "gate");
        this.nodeSession = nodeSession;
        this.profiles = profiles;
        Objects.requireNonNull(interval, "interval");
        this.nativeViewTtl = profiles == null ? null : interval.multipliedBy(3);
        this.reconciler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-cluster-membership-reconciler");
            thread.setDaemon(true);
            return thread;
        });
        reconciler.scheduleWithFixedDelay(this::reconcile, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private ClusterMembershipController() {
        this.clusterId = "inactive";
        this.member = null;
        this.store = null;
        this.gate = null;
        this.nodeSession = null;
        this.profiles = null;
        this.nativeViewTtl = null;
        this.reconciler = null;
        this.closed.set(true);
    }

    static ClusterMembershipController inactive() {
        return new ClusterMembershipController();
    }

    void reconcile() {
        if (closed.get()) {
            return;
        }
        try {
            List<Member> sampled = List.copyOf(member.getCluster().getMembers());
            ClusterNativeObservation observation = null;
            Set<String> visible;
            if (profiles != null && nodeSession != null && nodeSession.profileGeneration() > 0) {
                ClusterExecutionProfile profile = profiles.profile(clusterId).orElse(null);
                observation = nativeObservation(sampled, profile).orElse(null);
                if (observation == null) {
                    gate.failClosed();
                    return;
                }
                visible = observation.membersByNode().keySet();
            } else {
                visible = ClusterMembershipGate.nodeIds(sampled);
            }
            Optional<ClusterMembership> stored = store.read(clusterId);
            if (nodeSession != null && nodeSession.profileGeneration() > 0) {
                reconcileProfile(visible, stored);
                ClusterMembership committed = gate.committed();
                if (observation != null && committed != null
                        && committed.profileGeneration() == nodeSession.profileGeneration()) {
                    // This reports the whole sampled pool even below quorum. Only the store's later
                    // admission guard can qualify fresh agreeing views against the committed majority.
                    profiles.recordNativeView(nodeSession, committed.revision(), observation, nativeViewTtl);
                }
                return;
            }
            if (stored.isEmpty()) {
                if (gate.canCommit(visible)) {
                    gate.install(store.createIfAbsent(clusterId, visible));
                } else {
                    gate.failClosed();
                }
                return;
            }

            ClusterMembership current = stored.get();
            gate.install(current);
            if (joinsOnly(current.activeNodeIds(), visible) && gate.canCommit(visible)) {
                ClusterMembership next = store.compareAndSet(clusterId, current.revision(), visible)
                        .orElseGet(() -> store.read(clusterId).orElseThrow());
                gate.install(next);
            } else {
                gate.canCommit(visible);
            }
        } catch (RuntimeException unavailable) {
            gate.failClosed();
            LOG.warn("Could not refresh committed cluster membership; business work remains fail-closed", unavailable);
        }
    }

    private Optional<ClusterNativeObservation> nativeObservation(List<Member> sampled, ClusterExecutionProfile profile) {
        if (profile == null || !profile.clusterId().equals(clusterId)
                || profile.generation() != nodeSession.profileGeneration()
                || nodeSession.key().type() != WorkloadClaimType.NODE_SESSION
                || !nodeSession.key().clusterId().equals(clusterId)
                || !nodeSession.key().resourceId().equals(nodeSession.owner().nodeId())
                || !ExecutionProfileFactory.PROFILE_PROTOCOL.equals(profile.profile().attributes().get("profile-protocol"))) {
            return Optional.empty();
        }
        Map<String, ClusterNativeMember> observed = new TreeMap<>();
        Set<String> uuids = new HashSet<>();
        Set<String> addresses = new HashSet<>();
        boolean localFound = false;
        for (Member peer : sampled) {
            String node = peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE);
            String boot = peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
            var uuid = peer.getUuid();
            var address = peer.getAddress();
            if (peer.isLiteMember() || node == null || node.isBlank() || boot == null || boot.isBlank()
                    || uuid == null || address == null || address.toString().isBlank()
                    || !Long.toString(profile.generation()).equals(peer.getAttribute(ClusterMembershipGate.PROFILE_GENERATION_ATTRIBUTE))
                    || !profile.profile().hash().equals(peer.getAttribute(ClusterMembershipGate.PROFILE_HASH_ATTRIBUTE))
                    || !uuids.add(uuid.toString()) || !addresses.add(address.toString())) {
                return Optional.empty();
            }
            var actual = new ClusterNativeMember(new ClusterExecutionMember(node, boot, uuid.toString()), address.toString());
            if (observed.putIfAbsent(node, actual) != null) { return Optional.empty(); }
            if (peer.localMember()) {
                if (localFound || !nodeSession.owner().nodeId().equals(node)
                        || !nodeSession.owner().bootId().equals(boot)) { return Optional.empty(); }
                localFound = true;
            }
        }
        return localFound ? Optional.of(new ClusterNativeObservation(profile, observed)) : Optional.empty();
    }

    private void reconcileProfile(Set<String> visible, Optional<ClusterMembership> stored) {
        long generation = nodeSession.profileGeneration();
        if (stored.isEmpty() || stored.get().profileGeneration() < generation) {
            gate.failClosed();
            if (gate.canCommit(visible)) {
                Optional<ClusterMembership> initialized = store.initializeProfile(nodeSession,
                        stored.map(ClusterMembership::revision).orElse(0L), visible);
                initialized.ifPresentOrElse(gate::install, gate::failClosed);
            }
            return;
        }
        ClusterMembership current = stored.get();
        if (current.profileGeneration() != generation) {
            gate.failClosed();
            return;
        }
        gate.install(current);
        if (joinsOnly(current.activeNodeIds(), visible) && gate.canCommit(visible)) {
            // A refused join can leave this profile current or lose a revision race.
            // The stored membership still decides the full committed majority.
            store.compareAndSetProfile(nodeSession, current.revision(), visible)
                    .or(() -> store.read(clusterId)
                            .filter(latest -> latest.profileGeneration() == generation))
                    .ifPresentOrElse(gate::install, gate::failClosed);
        } else {
            gate.canCommit(visible);
        }
    }

    /**
     * Whether {@code visible} only adds to what is committed, which is the only change this member makes
     * on its own.
     *
     * <p><b>A member missing from this view has not been shown to be gone.</b> Unreachable is the only
     * thing a member is ever told, and a peer that died and a peer behind a cut are the same reading. So
     * committing a set with a member dropped out of it is committing a guess, and the guess is the one
     * that matters: eligibility is a strict majority of the committed set, so every member dropped
     * lowers the bar the next drop has to clear. Four members cut two against two lose their peers one
     * at a time -- three is a majority of four, then two is a majority of three -- and a side that could
     * never have qualified against the four has made itself a majority of a set it wrote itself, while
     * the other side, now overlapping nothing, is refused. Both sides are equally entitled to do this,
     * so which one acts is decided by which sampled the moment between two departures, and nothing about
     * it is an invariant.
     *
     * <p>Growing is not the mirror of this and is left alone: a member that is here is here, whoever
     * cannot see it, and admitting it needs every already-committed member present to admit it.
     *
     * <p>The cost is deliberate and is not self-healing: members that really are gone stay in the set,
     * so a cluster that permanently loses a majority stays refused until something outside this loop
     * commits a smaller set. Refusing to act is recoverable; two sides acting on one pipeline is not.
     */
    private static boolean joinsOnly(Set<String> committed, Set<String> visible) {
        return visible.containsAll(committed) && !visible.equals(committed);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true) || reconciler == null) {
            return;
        }
        reconciler.shutdownNow();
    }
}
