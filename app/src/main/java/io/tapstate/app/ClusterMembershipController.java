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

/** Commits bootstrap/add/remove topology revisions only from a view qualified by the prior revision. */
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
            if (!current.activeNodeIds().equals(visible) && gate.canCommit(visible)) {
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

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true) || reconciler == null) {
            return;
        }
        reconciler.shutdownNow();
    }
}
