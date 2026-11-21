package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimRenewalException;
import io.tapstate.spi.store.WorkloadClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Keeps a pre-join node-session claim live and removes the member when that proof is lost.
 *
 * A transient store failure leaves the last confirmed proof in force for its remaining time.
 * A refusal or the local proof deadline removes the member even if a store call is still active.
 */
final class NodeSessionLease implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeSessionLease.class);

    /** The longest a lapsed session goes unnoticed, when the renewal interval does not look sooner. */
    private static final Duration LAPSE_CHECK = Duration.ofSeconds(1);
    private static final long ATTEMPT_NANOS = Duration.ofSeconds(5).toNanos();
    private static final long RETRY_NANOS = Duration.ofMillis(500).toNanos();

    private final WorkloadClaimStore store;
    private final LongSupplier nanoTime;
    private final AtomicReference<Proof> current;
    private final Duration ttl;
    private final long intervalNanos;
    private final Runnable lost;
    private final ScheduledExecutorService renewer;
    private long attempt;

    /** The claim and its monotonic deadline are published and fenced together. */
    private record Proof(WorkloadClaim claim, long until) {}

    private NodeSessionLease() {
        this.nanoTime = System::nanoTime;
        this.store = null;
        this.current = new AtomicReference<>();
        this.ttl = Duration.ZERO;
        this.intervalNanos = 0;
        this.lost = () -> { };
        this.renewer = null;
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
        long initialDeadline = askedAt + ttl.toNanos();
        this.current = new AtomicReference<>(new Proof(Objects.requireNonNull(initial, "initial"), initialDeadline));
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.lost = Objects.requireNonNull(lost, "lost");
        this.intervalNanos = renewInterval.toNanos();
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
                millis(initialDeadline - adoptedAt), ttl.toMillis(), renewInterval.toMillis());
        CountDownLatch registered = new CountDownLatch(1);
        try {
            renewer.execute(() -> afterRegistration(registered, () -> renew(adoptedAt, false)));
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

    private void renew(long due, boolean confirm) {
        Proof prior = liveProof();
        if (prior == null) return;
        WorkloadClaim expected = prior.claim();
        long askedAt = nanoTime.getAsLong();
        long budget = Math.min(ATTEMPT_NANOS, prior.until() - askedAt);
        if (budget <= 0) {
            liveProof();
            return;
        }
        long number = ++attempt;
        String operation = confirm ? "confirm" : "renew";
        LOG.debug("Node session {} started [attempt={}, schedule_delay_ms={}, budget_ms={}, proof_remaining_ms={}].",
                operation, number, millis(askedAt - due), millis(budget), millis(prior.until() - askedAt));
        Proof next;
        try {
            long deadline = askedAt + budget;
            next = confirm ? confirm(prior, askedAt, deadline)
                    : bounded(expected, deadline, () -> store.renew(expected, ttl))
                            .map(claim -> new Proof(claim, askedAt + ttl.toNanos())).orElse(null);
        } catch (RuntimeException failure) {
            long returnedAt = nanoTime.getAsLong();
            LOG.debug("Node session {} returned [attempt={}, elapsed_ms={}, outcome={}, proof_remaining_ms={}].",
                    operation, number, millis(returnedAt - askedAt), outcome(failure), millis(prior.until() - returnedAt));
            if (store instanceof DeadlineWorkloadClaimStore && retryable(failure)) {
                boolean newResultUncertain = !(failure instanceof WorkloadClaimRenewalException claimFailure)
                        || claimFailure.reason() == WorkloadClaimRenewalException.Reason.UNCONFIRMED;
                scheduleRetry(prior, returnedAt, confirm || newResultUncertain);
            } else {
                lose(prior, "the coordination store could not renew the node session", failure);
            }
            return;
        }
        long returnedAt = nanoTime.getAsLong();
        LOG.debug("Node session {} returned [attempt={}, elapsed_ms={}, outcome={}, proof_remaining_ms={}].",
                operation, number, millis(returnedAt - askedAt),
                next == null ? "refused" : next == prior ? "unchanged" : "extended", millis(prior.until() - returnedAt));
        if (next == null) {
            lose(prior, "the stored node session is no longer live under the original fence", null);
            return;
        }
        if (next == prior) {
            scheduleRetry(prior, returnedAt, false);
            return;
        }
        long publishedAt = nanoTime.getAsLong();
        if (publishedAt - prior.until() >= 0 || publishedAt - next.until() >= 0) {
            lose(prior, "the renewal arrived after the prior proof expired", null);
            return;
        }
        if (!current.compareAndSet(prior, next)) return;
        LOG.debug("Node session {} published [attempt={}, publish_ms={}, elapsed_ms={}, proof_remaining_ms={}].",
                operation, number, millis(nanoTime.getAsLong() - returnedAt),
                millis(publishedAt - askedAt), millis(next.until() - publishedAt));
        long cadence = confirm ? Math.min(intervalNanos, (next.until() - publishedAt) / 2) : intervalNanos;
        schedule(askedAt + cadence, false);
    }

    private Proof confirm(Proof prior, long askedAt, long deadline) {
        WorkloadClaim expected = prior.claim();
        var reading = bounded(expected, deadline, () -> store.read(expected.key()));
        if (reading.isEmpty()) return null;
        var observed = reading.orElseThrow();
        if (!observed.leased() || !WorkloadClaimFence.from(expected).equals(WorkloadClaimFence.from(observed.claim()))) {
            return null;
        }
        if (!observed.claim().leaseUntil().isAfter(expected.leaseUntil())) return prior;
        long remaining = Math.min(ttl.toNanos(), observed.leaseRemaining().toNanos());
        return new Proof(observed.claim(), askedAt + remaining);
    }

    private <T> T bounded(WorkloadClaim expected, long deadline, Supplier<T> operation) {
        if (store instanceof DeadlineWorkloadClaimStore bounded) {
            return bounded.withNodeSessionDeadline(expected.key(), deadline, this::requireActive, operation);
        }
        return operation.get();
    }

    private void requireActive() {
        if (!active()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, java.util.Map.of(), null);
    }

    private static boolean retryable(RuntimeException failure) {
        return failure instanceof WorkloadClaimRenewalException
                || failure instanceof TapstateException coded && (coded.code() == IoError.STORE_UNAVAILABLE
                || coded.code() == IoError.WORKLOAD_CLAIM_FENCED && coded.getCause() instanceof TimeoutException);
    }

    private static String outcome(RuntimeException failure) {
        if (failure instanceof WorkloadClaimRenewalException renewal) return renewal.reason().name();
        if (failure instanceof TapstateException coded) return coded.code().code();
        return failure.getClass().getSimpleName();
    }

    private static long millis(long nanos) { return TimeUnit.NANOSECONDS.toMillis(nanos); }

    private void scheduleRetry(Proof prior, long now, boolean confirm) {
        if (now - prior.until() >= 0) {
            liveProof();
        } else {
            schedule(now + Math.min(RETRY_NANOS, prior.until() - now), confirm);
        }
    }

    private void schedule(long due, boolean confirm) {
        if (current.get() == null) return;
        try {
            renewer.schedule(() -> renew(due, confirm), Math.max(0, due - nanoTime.getAsLong()), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException stopping) {
            if (current.get() != null) throw stopping;
        }
    }

    /** Fixed input for a new controlled operation; CACHE still proves this exact live claim by a real write. */
    Optional<WorkloadClaimFence> currentFence() {
        Proof proof = liveProof();
        if (proof == null) return Optional.empty();
        WorkloadClaimFence captured = WorkloadClaimFence.from(proof.claim());
        return active() ? Optional.of(captured) : Optional.empty();
    }

    /** A non-blocking local predicate; an observed expiry is irreversible even if shutdown blocks. */
    boolean active() { return liveProof() != null; }

    private Proof liveProof() {
        for (;;) {
            Proof proof = current.get();
            if (proof == null) return null;
            if (nanoTime.getAsLong() - proof.until() < 0) {
                if (current.get() == proof) return proof;
                continue;
            }
            if (current.compareAndSet(proof, null)) {
                renewer.execute(() -> stopAfterLoss("the original node-session proof expired", null));
                return null;
            }
        }
    }

    /** The startup listener shares the same irreversible local proof as runtime admission. */
    boolean isCurrent() { return active(); }

    private void loseALapsedSession() { liveProof(); }

    private void lose(Proof prior, String reason, RuntimeException cause) {
        if (current.compareAndSet(prior, null)) stopAfterLoss(reason, cause);
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
        Proof ending = current.getAndSet(null);
        try {
            stop.run();
        } finally {
            if (ending != null) {
                renewer.shutdownNow();
                release(ending.claim());
            }
        }
    }

    /** A library lifecycle listener cannot wait for coordination-store I/O. */
    void closeAfterMemberStops() {
        Proof ending = current.getAndSet(null);
        if (ending == null) return;
        renewer.execute(() -> {
            try {
                release(ending.claim());
            } finally {
                renewer.shutdownNow();
            }
        });
        renewer.shutdown();
    }

    private void release(WorkloadClaim ending) {
        try {
            store.release(ending);
        } catch (RuntimeException unavailable) {
            LOG.warn("Could not release the node session during shutdown; it will expire by lease.", unavailable);
        }
    }
}
