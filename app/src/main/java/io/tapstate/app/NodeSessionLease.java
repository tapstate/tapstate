package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

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
    private final LongSupplier nanoTime;
    private final AtomicReference<WorkloadClaim> current;
    private final Duration ttl;
    private final Runnable lost;
    private final ScheduledExecutorService renewer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean firstRenewal = new AtomicBoolean(true);

    /** When the lease the last accepted renewal bought runs out, on the monotonic clock. */
    private volatile long provenUntil;

    private NodeSessionLease() {
        this.nanoTime = System::nanoTime;
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
        this(store, initial, askedAt, ttl, renewInterval, lost, System::nanoTime);
    }

    NodeSessionLease(WorkloadClaimStore store, WorkloadClaim initial, long askedAt, Duration ttl, Duration renewInterval,
                     Runnable lost, LongSupplier nanoTime) {
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
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
        long adoptedAt = nanoTime.getAsLong();
        LOG.info("Node session maintenance adopted [proof_age_ms={}, proof_remaining_ms={}, ttl_ms={}, renew_interval_ms={}].",
                TimeUnit.NANOSECONDS.toMillis(adoptedAt - askedAt),
                TimeUnit.NANOSECONDS.toMillis(provenUntil - adoptedAt), ttl.toMillis(), renewInterval.toMillis());
        CountDownLatch registered = new CountDownLatch(1);
        try {
            renewer.scheduleWithFixedDelay(() -> afterRegistration(registered, this::renew),
                    0, renewInterval.toMillis(), TimeUnit.MILLISECONDS);
            long lapseCheck = Math.max(1L, Math.min(renewInterval.toMillis(), LAPSE_CHECK.toMillis()));
            renewer.scheduleWithFixedDelay(() -> afterRegistration(registered, this::loseALapsedSession),
                    lapseCheck, lapseCheck, TimeUnit.MILLISECONDS);
        } catch (RuntimeException | Error setupFailure) {
            renewer.shutdownNow();
            throw setupFailure;
        } finally {
            // A fast first refusal must not shut down the executor while its other task is being registered.
            registered.countDown();
        }
    }

    private static void afterRegistration(CountDownLatch registered, Runnable action) {
        try {
            registered.await();
        } catch (InterruptedException cancelled) {
            Thread.currentThread().interrupt();
            return;
        }
        action.run();
    }

    private void renew() {
        if (!active()) return;
        long priorProofUntil = provenUntil;
        WorkloadClaim expected = current.get();
        long askedAt = nanoTime.getAsLong();
        boolean first = firstRenewal.compareAndSet(true, false);
        if (first) {
            LOG.info("Node session first renewal started [proof_remaining_ms={}].",
                    TimeUnit.NANOSECONDS.toMillis(priorProofUntil - askedAt));
        }
        try {
            Optional<WorkloadClaim> renewed = store.renew(expected, ttl);
            long storeReturnedAt = nanoTime.getAsLong();
            if (first) {
                LOG.info("Node session first renewal store returned [elapsed_ms={}, matched={}].",
                        TimeUnit.NANOSECONDS.toMillis(storeReturnedAt - askedAt), renewed.isPresent());
            }
            if (renewed.isPresent()) {
                if (!active()) return;
                if (nanoTime.getAsLong() - priorProofUntil >= 0) {
                    lose("the renewal arrived after the prior proof expired", null); return;
                }
                WorkloadClaim accepted = renewed.get();
                current.set(accepted);
                provenUntil = askedAt + ttl.toNanos();
                if (first) {
                    long completedAt = nanoTime.getAsLong();
                    LOG.info("Node session first renewal accepted [publish_ms={}, elapsed_ms={}, proof_remaining_ms={}].",
                            TimeUnit.NANOSECONDS.toMillis(completedAt - storeReturnedAt),
                            TimeUnit.NANOSECONDS.toMillis(completedAt - askedAt),
                            TimeUnit.NANOSECONDS.toMillis(provenUntil - completedAt));
                }
                return;
            }
            lose("the stored owner or generation no longer matches", null);
        } catch (RuntimeException unavailable) {
            lose("the coordination store could not renew the node session", unavailable);
        }
    }

    /** Fixed input for a new controlled operation; CACHE still proves this exact live claim by a real write. */
    Optional<WorkloadClaimFence> currentFence() {
        if (!active()) return Optional.empty();
        WorkloadClaimFence captured = WorkloadClaimFence.from(current.get());
        return active() ? Optional.of(captured) : Optional.empty();
    }

    /** A non-blocking local predicate; an observed expiry is irreversible even if shutdown blocks. */
    boolean active() {
        if (closed.get()) return false;
        if (nanoTime.getAsLong() - provenUntil >= 0) {
            if (closed.compareAndSet(false, true)) {
                renewer.execute(() -> stopAfterLoss("the original node-session proof expired", null));
            }
            return false;
        }
        return !closed.get();
    }

    /** The startup listener shares the same irreversible local proof as runtime admission. */
    boolean isCurrent() { return active(); }

    private void loseALapsedSession() {
        if (nanoTime.getAsLong() - provenUntil >= 0) {
            lose("no renewal was accepted within the lease the last one bought", null);
        }
    }

    private void lose(String reason, RuntimeException cause) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        stopAfterLoss(reason, cause);
    }

    private void stopAfterLoss(String reason, RuntimeException cause) {
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
        stopAndClose(() -> { });
    }

    /** Fence locally before stopping, and never put coordination I/O ahead of the member stop. */
    void stopAndClose(Runnable stop) {
        boolean release = store != null && closed.compareAndSet(false, true);
        try {
            stop.run();
        } finally {
            if (release) {
                renewer.shutdownNow();
                releaseCurrent();
            }
        }
    }

    /** A library lifecycle listener cannot wait for coordination-store I/O. */
    void closeAfterMemberStops() {
        if (store == null || !closed.compareAndSet(false, true)) return;
        renewer.execute(() -> {
            try {
                releaseCurrent();
            } finally {
                renewer.shutdownNow();
            }
        });
        renewer.shutdown();
    }

    private void releaseCurrent() {
        WorkloadClaim ending = current.get();
        try {
            store.release(ending);
        } catch (RuntimeException unavailable) {
            LOG.warn("Could not release the node session during shutdown; it will expire by lease.", unavailable);
        }
    }
}
