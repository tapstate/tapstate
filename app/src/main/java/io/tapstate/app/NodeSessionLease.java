package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Keeps a pre-join node-session claim live and removes the member when that proof is lost.
 *
 * <p>Lost is either of two things. The store says so -- a renewal finds the claim no longer this
 * member's, or cannot be made -- or the store says nothing for longer than the lease the last renewal it
 * accepted bought. The second is timed here, on this member's monotonic clock and from the moment that
 * renewal was asked for: nothing this member did can have carried the lease past that, so once it has
 * gone by the session is not one this member can prove. Waiting instead for the renewal in flight to
 * answer keeps a member in the cluster for as long as the store stays silent, and a store that stalls
 * rather than refuses leaves that call hanging with no end -- the member working on, and answering for
 * the cluster, the whole time and for a moment after the store comes back.
 */
final class NodeSessionLease implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeSessionLease.class);

    /** The longest a lapsed session goes unnoticed, when the renewal interval does not look sooner. */
    private static final Duration LAPSE_CHECK = Duration.ofSeconds(1);

    private final WorkloadClaimStore store;
    private final AtomicReference<WorkloadClaim> current;
    private final Duration ttl;
    private final Runnable lost;
    private final io.tapstate.spi.store.ClusterProfileStore profiles;
    private final java.util.function.Supplier<JoinedIdentity> nativeIdentity;
    private JoinedIdentity publishedIdentity;
    private final ScheduledExecutorService renewer;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** When the lease the last accepted renewal bought runs out, on the monotonic clock. */
    private volatile long provenUntil;

    private NodeSessionLease() {
        this.store = null;
        this.current = null;
        this.ttl = Duration.ZERO;
        this.lost = () -> { };
        this.profiles = null;
        this.nativeIdentity = null;
        this.publishedIdentity = null;
        this.renewer = null;
        this.closed.set(true);
    }

    static NodeSessionLease inactive() {
        return new NodeSessionLease();
    }

    /** This boot's last proven profile/session and its conservative local authorization bound. */
    record Proof(WorkloadClaim claim, long deadlineNanos, boolean live) { }

    /** Actual local runtime identity, separate from its immutable node-session admission identity. */
    record JoinedIdentity(String memberUuid, String memberAddress, String nodeId, String bootId,
            String profileGeneration, String profileHash) {
        JoinedIdentity {
            Objects.requireNonNull(memberUuid, "memberUuid");
            Objects.requireNonNull(memberAddress, "memberAddress");
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(bootId, "bootId");
            Objects.requireNonNull(profileGeneration, "profileGeneration");
            Objects.requireNonNull(profileHash, "profileHash");
        }
        boolean sameAdmission(JoinedIdentity other) {
            return nodeId.equals(other.nodeId) && bootId.equals(other.bootId)
                    && profileGeneration.equals(other.profileGeneration) && profileHash.equals(other.profileHash);
        }
        boolean names(WorkloadClaim claim) {
            return nodeId.equals(claim.owner().nodeId()) && bootId.equals(claim.owner().bootId())
                    && profileGeneration.equals(Long.toString(claim.profileGeneration()));
        }
    }

    Proof proof() {
        if (current == null) {
            return null;
        }
        long deadline = provenUntil;
        return new Proof(current.get(), deadline, !closed.get() && System.nanoTime() - deadline < 0);
    }

    /**
     * Keeps {@code initial} live from now on. {@code askedAt} is when, on the monotonic clock, the
     * acquisition that produced it was asked for -- no later -- which is where its lease is timed from.
     */
    NodeSessionLease(
            WorkloadClaimStore store,
            WorkloadClaim initial,
            long askedAt,
            Duration ttl,
            Duration renewInterval,
            Runnable lost) {
        this(store, initial, askedAt, ttl, renewInterval, lost, null, null, null, false);
    }

    NodeSessionLease(WorkloadClaimStore store, WorkloadClaim initial, long askedAt, Duration ttl,
            Duration renewInterval, Runnable lost, io.tapstate.spi.store.ClusterProfileStore profiles,
            java.util.function.Supplier<JoinedIdentity> nativeIdentity, JoinedIdentity publishedIdentity) {
        this(store, initial, askedAt, ttl, renewInterval, lost, profiles, nativeIdentity, publishedIdentity, true);
    }

    private NodeSessionLease(WorkloadClaimStore store, WorkloadClaim initial, long askedAt, Duration ttl,
            Duration renewInterval, Runnable lost, io.tapstate.spi.store.ClusterProfileStore profiles,
            java.util.function.Supplier<JoinedIdentity> nativeIdentity, JoinedIdentity publishedIdentity,
            boolean refreshJoin) {
        this.store = Objects.requireNonNull(store, "store");
        this.current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.lost = Objects.requireNonNull(lost, "lost");
        if (refreshJoin && initial.profileGeneration() > 0 && profiles == null) {
            throw new io.tapstate.core.common.TapstateException(BootError.COORDINATION_STORE_REQUIRED, java.util.Map.of(), null);
        }
        this.profiles = refreshJoin && initial.profileGeneration() > 0 ? profiles : null;
        this.nativeIdentity = this.profiles == null ? null : Objects.requireNonNull(nativeIdentity, "nativeIdentity");
        this.publishedIdentity = this.profiles == null ? null : Objects.requireNonNull(publishedIdentity, "publishedIdentity");
        if (this.publishedIdentity != null && !this.publishedIdentity.names(initial)) {
            throw joinLost();
        }
        Objects.requireNonNull(renewInterval, "renewInterval");
        this.provenUntil = askedAt + ttl.toNanos();
        // Two threads: a renewal the store never answers holds one of them, and the lapse still has to be
        // noticed on the other.
        this.renewer = Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "tapstate-node-session-renewer");
            thread.setDaemon(true);
            return thread;
        });
        // Register the lapse check before an immediate refusal can shut down this executor.
        long lapseCheck = Math.max(1L, Math.min(renewInterval.toMillis(), LAPSE_CHECK.toMillis()));
        renewer.scheduleWithFixedDelay(this::loseALapsedSession, lapseCheck, lapseCheck, TimeUnit.MILLISECONDS);
        // Member formation may have consumed most of the original acquisition's remaining lease.
        renewer.scheduleWithFixedDelay(
                this::renew, 0L, renewInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void renew() {
        if (closed.get()) {
            return;
        }
        WorkloadClaim expected = current.get();
        long askedAt = System.nanoTime();
        try {
            Optional<WorkloadClaim> renewed = store.renew(expected, ttl);
            if (renewed.isPresent()) {
                if (closed.get()) return;
                refreshChangedJoin(expected, renewed.get());
                if (closed.get()) return;
                current.set(renewed.get());
                provenUntil = askedAt + ttl.toNanos();
                return;
            }
            lose("the stored owner or generation no longer matches", null);
        } catch (RuntimeException unavailable) {
            lose("the coordination store could not renew the node session or refresh its join proof", unavailable);
        }
    }

    private void refreshChangedJoin(WorkloadClaim expected, WorkloadClaim accepted) {
        if (profiles == null) return;
        if (!io.tapstate.spi.store.WorkloadClaimFence.from(expected).equals(io.tapstate.spi.store.WorkloadClaimFence.from(accepted))) {
            throw joinLost();
        }
        JoinedIdentity actual = nativeIdentity.get();
        if (!actual.names(accepted) || !publishedIdentity.sameAdmission(actual)) throw joinLost();
        if (!actual.equals(publishedIdentity)) {
            if (!profiles.markJoined(accepted, actual.memberUuid(), actual.memberAddress())) throw joinLost();
            if (!actual.equals(nativeIdentity.get())) throw joinLost();
            publishedIdentity = actual;
        }
    }

    private static io.tapstate.core.common.TapstateException joinLost() {
        return new io.tapstate.core.common.TapstateException(BootError.PROFILE_SESSION_LOST, java.util.Map.of(), null);
    }

    private void loseALapsedSession() {
        if (System.nanoTime() - provenUntil >= 0) {
            lose("no renewal was accepted within the lease the last one bought", null);
        }
    }

    private void lose(String reason, RuntimeException cause) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (cause == null) {
            LOG.error("Node session lost: {}. Shutting down the Hazelcast member.", reason);
        } else {
            LOG.error("Node session lost: {}. Shutting down the Hazelcast member.", reason, cause);
        }
        try {
            lost.run();
        } finally {
            // After the stop, not before. This runs on a renewer thread and shutting the renewer down
            // interrupts it, while a graceful stop waits -- on the partitions it hands over, on its own
            // services -- and would return at the first wait. What is still worth interrupting is the other
            // renewer thread, which a renewal the store never answered may be holding.
            renewer.shutdownNow();
        }
    }

    @Override
    public void close() {
        if (store == null) {
            return;
        }
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        renewer.shutdownNow();
        try {
            store.release(current.get());
        } catch (RuntimeException unavailable) {
            LOG.warn("Could not release the node session during shutdown; it will expire by lease.", unavailable);
        }
    }
}
