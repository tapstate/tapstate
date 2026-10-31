package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.spi.store.ValidationClaims;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Local authorization for one fixed validation owner tenure. All its runs share this proof, but retain
 * separate run identities. Losing proof ends the tenure permanently; this object never acquires or
 * advances a claim. The caller must fence result publication separately in its actual storage domain.
 */
final class ValidationOwnerLease implements AutoCloseable {
    enum LossReason { EXPIRED, NODE_NOT_AUTHORIZED, RENEW_REFUSED, RENEW_FAILED, SCHEDULING_FAILED, CLOSED }

    private final Object lock = new Object();
    private final WorkloadClaim original;
    private final ValidationOwner owner;
    private final WorkloadClaimFence fence;
    private final long ttlNanos;
    private final LongSupplier nanoTime;
    private final BooleanSupplier nodeAuthorized;
    private final Function<WorkloadClaim, Optional<WorkloadClaim>> renew;
    private final Consumer<WorkloadClaim> release;
    private final Consumer<LossReason> lost;
    private final AtomicBoolean releaseAttempted = new AtomicBoolean();
    private volatile boolean active = true;
    private volatile long provenUntil;
    private volatile LossReason lossReason;
    private boolean renewing;
    private boolean scheduled;
    private ScheduledFuture<?> renewalTask;
    private ScheduledFuture<?> deadlineTask;

    /**
     * askedAt is captured before the successful acquire/renew request, never after its response.
     * renew/release must use the committed-membership facade; nodeAuthorized is a local, nonblocking
     * node-session guard. The loss callback cancels only this owner's validation work, never the member
     * or the production Pipeline. Executors and actual query resources remain owned by the caller.
     */
    ValidationOwnerLease(String pipelineId, WorkloadClaim initial, long askedAt, Duration ttl,
            LongSupplier nanoTime, BooleanSupplier nodeAuthorized,
            Function<WorkloadClaim, Optional<WorkloadClaim>> renew,
            Consumer<WorkloadClaim> release, Consumer<LossReason> lost) {
        this.original = Objects.requireNonNull(initial, "initial");
        this.owner = ValidationClaims.owner(pipelineId, initial);
        this.fence = ValidationClaims.fence(owner);
        positiveNanos(ttl);
        if (ttl.toMillis() == 0) throw new IllegalArgumentException("validation lease TTL must be at least one millisecond");
        // The durable store truncates to milliseconds. Local proof must never retain the remainder.
        this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttl.toMillis());
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.nodeAuthorized = Objects.requireNonNull(nodeAuthorized, "nodeAuthorized");
        this.renew = Objects.requireNonNull(renew, "renew");
        this.release = Objects.requireNonNull(release, "release");
        this.lost = Objects.requireNonNull(lost, "lost");
        this.provenUntil = askedAt + ttlNanos;
    }

    ValidationOwner owner() { return owner; }
    LossReason lossReason() { return lossReason; }

    WorkloadClaimFence require() {
        if (!authorized()) {
            throw new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED,
                    Map.of("pipeline", owner.pipelineId()), null);
        }
        return fence;
    }

    /** Cheap guard before every external call and after blocking reads, with no store call. */
    boolean authorized() {
        if (!active) return false;
        if (expired()) {
            lose(LossReason.EXPIRED);
            return false;
        }
        boolean nodeLive;
        try {
            nodeLive = nodeAuthorized.getAsBoolean();
        } catch (TapstateException unknown) {
            nodeLive = false;
        } catch (RuntimeException | Error defect) {
            throw loseAfterFailure(LossReason.NODE_NOT_AUTHORIZED, defect);
        }
        if (!nodeLive) {
            lose(LossReason.NODE_NOT_AUTHORIZED);
            return false;
        }
        if (expired()) lose(LossReason.EXPIRED);
        return active;
    }

    /** The deadline task remains independent of any blocked renewal. */
    void checkDeadline() { authorized(); }

    void renew() {
        if (!authorized()) return;
        long askedAt;
        boolean expiredBeforeCall;
        synchronized (lock) {
            if (!active || renewing) return;
            expiredBeforeCall = expired();
            if (expiredBeforeCall) {
                askedAt = 0;
            } else {
                renewing = true;
                askedAt = nanoTime.getAsLong();
            }
        }
        if (expiredBeforeCall) {
            lose(LossReason.EXPIRED);
            return;
        }
        try {
            Optional<WorkloadClaim> answer;
            try {
                answer = Objects.requireNonNull(renew.apply(original), "renewal answer");
            } catch (TapstateException unavailable) {
                lose(LossReason.RENEW_FAILED);
                return;
            } catch (RuntimeException | Error defect) {
                throw loseAfterFailure(LossReason.RENEW_FAILED, defect);
            }
            if (!authorized()) return;
            if (answer.isEmpty() || !WorkloadClaimFence.from(answer.get()).equals(fence)) {
                lose(LossReason.RENEW_REFUSED);
                return;
            }
            try {
                if (!ValidationClaims.owner(owner.pipelineId(), answer.get()).equals(owner)) {
                    lose(LossReason.RENEW_REFUSED);
                    return;
                }
            } catch (TapstateException invalidContext) {
                throw loseAfterFailure(LossReason.RENEW_REFUSED, invalidContext);
            }
            boolean lapsed;
            synchronized (lock) {
                if (!active) return;
                long now = nanoTime.getAsLong();
                lapsed = now - provenUntil >= 0 || now - askedAt >= ttlNanos;
                if (!lapsed) provenUntil = askedAt + ttlNanos;
            }
            if (lapsed) lose(LossReason.EXPIRED);
        } finally {
            synchronized (lock) { renewing = false; }
        }
    }

    /** Both executors are bounded shared resources supplied by the validation runtime. */
    void maintain(ScheduledExecutorService renewals, ScheduledExecutorService deadlines,
            Duration renewalInterval, Duration deadlineInterval) {
        Objects.requireNonNull(renewals, "renewals");
        Objects.requireNonNull(deadlines, "deadlines");
        long renewalNanos = positiveNanos(renewalInterval);
        long deadlineNanos = positiveNanos(deadlineInterval);
        if (renewals == deadlines || renewalNanos >= ttlNanos
                || deadlineNanos > Math.min(ttlNanos, TimeUnit.SECONDS.toNanos(1))) {
            throw new IllegalArgumentException("validation renewal and expiry need independent bounded schedules");
        }
        if (!authorized()) return;
        try {
            synchronized (lock) {
                if (!active) return;
                if (scheduled) throw new IllegalStateException("validation lease maintenance already registered");
                scheduled = true;
                renewalTask = renewals.scheduleWithFixedDelay(this::renew, renewalNanos,
                        renewalNanos, TimeUnit.NANOSECONDS);
                deadlineTask = deadlines.scheduleWithFixedDelay(this::checkDeadline, deadlineNanos,
                        deadlineNanos, TimeUnit.NANOSECONDS);
            }
        } catch (RuntimeException | Error setupFailure) {
            throw loseAfterFailure(LossReason.SCHEDULING_FAILED, setupFailure);
        }
    }

    private boolean expired() { return nanoTime.getAsLong() - provenUntil >= 0; }

    private void lose(LossReason reason) {
        synchronized (lock) {
            if (!active) return;
            lossReason = reason;
            active = false;
            if (renewalTask != null) renewalTask.cancel(true);
            if (deadlineTask != null) deadlineTask.cancel(false);
        }
        lost.accept(reason);
    }

    private RuntimeException loseAfterFailure(LossReason reason, Throwable primary) {
        var failure = new ValidationFailureLatch();
        failure.record(primary);
        try { lose(reason); }
        catch (RuntimeException | Error cleanup) {
            failure.record(cleanup);
            ValidationFailureLatch.addContext(failure.get(), cleanup);
        }
        Throwable dominant = failure.get();
        if (dominant instanceof Error fatal) throw fatal;
        return (RuntimeException) dominant;
    }

    /** Releasing an old exact tenure cannot release a successor, even after a late response. */
    void revoke() { lose(LossReason.CLOSED); }

    /** Releasing an old exact tenure cannot release a successor, even after a late response. */
    @Override public void close() {
        var failure = new ValidationFailureLatch();
        try { lose(LossReason.CLOSED); }
        catch (RuntimeException | Error cleanupFailure) { failure.record(cleanupFailure); }
        if (releaseAttempted.compareAndSet(false, true)) {
            try { release.accept(original); }
            catch (RuntimeException | Error releaseFailure) {
                failure.record(releaseFailure);
                ValidationFailureLatch.addContext(failure.get(), releaseFailure);
            }
        }
        Throwable dominant = failure.get();
        if (dominant instanceof Error fatal) throw fatal;
        if (dominant != null) throw (RuntimeException) dominant;
    }

    private static long positiveNanos(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (duration.isZero() || duration.isNegative() || duration.compareTo(Duration.ofDays(1)) > 0) {
            throw new IllegalArgumentException("validation lease durations must be positive and at most one day");
        }
        return duration.toNanos();
    }
}
