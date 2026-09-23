package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
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
    private final ScheduledExecutorService reconciler;
    private final AtomicBoolean closed = new AtomicBoolean();

    ClusterMembershipController(
            String clusterId,
            HazelcastInstance member,
            ClusterMembershipStore store,
            ClusterMembershipGate gate,
            Duration interval) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.member = Objects.requireNonNull(member, "member");
        this.store = Objects.requireNonNull(store, "store");
        this.gate = Objects.requireNonNull(gate, "gate");
        Objects.requireNonNull(interval, "interval");
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
            Set<String> visible = ClusterMembershipGate.nodeIds(member.getCluster().getMembers());
            Optional<ClusterMembership> stored = store.read(clusterId);
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
