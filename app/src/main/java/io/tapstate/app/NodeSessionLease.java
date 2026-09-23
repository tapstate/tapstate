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
    private final ScheduledExecutorService renewer;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** When the lease the last accepted renewal bought runs out, on the monotonic clock. */
    private volatile long provenUntil;

    private NodeSessionLease() {
        this.store = null;
        this.current = null;
        this.ttl = Duration.ZERO;
        this.lost = () -> { };
        this.renewer = null;
        this.closed.set(true);
    }

    static NodeSessionLease inactive() {
        return new NodeSessionLease();
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
        this.store = Objects.requireNonNull(store, "store");
        this.current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.lost = Objects.requireNonNull(lost, "lost");
        Objects.requireNonNull(renewInterval, "renewInterval");
        this.provenUntil = askedAt + ttl.toNanos();
        // Two threads: a renewal the store never answers holds one of them, and the lapse still has to be
        // noticed on the other.
        this.renewer = Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "tapstate-node-session-renewer");
            thread.setDaemon(true);
            return thread;
        });
        renewer.scheduleWithFixedDelay(
                this::renew, renewInterval.toMillis(), renewInterval.toMillis(), TimeUnit.MILLISECONDS);
        long lapseCheck = Math.max(1L, Math.min(renewInterval.toMillis(), LAPSE_CHECK.toMillis()));
        renewer.scheduleWithFixedDelay(this::loseALapsedSession, lapseCheck, lapseCheck, TimeUnit.MILLISECONDS);
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
                current.set(renewed.get());
                provenUntil = askedAt + ttl.toNanos();
                return;
            }
            lose("the stored owner or generation no longer matches", null);
        } catch (RuntimeException unavailable) {
            lose("the coordination store could not renew the node session", unavailable);
        }
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
        renewer.shutdownNow();
        lost.run();
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
