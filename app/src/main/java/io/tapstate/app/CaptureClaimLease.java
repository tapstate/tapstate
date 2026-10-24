package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Renews one capture claim from the moment it is taken, and stops its local capture immediately when
 * ownership can no longer be proved.
 *
 * <p>Renewing starts before the capture it guards is open, because opening one can take longer than a lease
 * -- a source slow to answer, many tables, an overloaded host -- and a claim nothing renewed meanwhile has run
 * out by the time the capture is open. What losing the claim stops is bound once there is something to stop;
 * a claim lost before that is answered as soon as it is bound.
 */
final class CaptureClaimLease implements AutoCloseable {

    private final CaptureOwnership ownership;
    private final AtomicReference<WorkloadClaim> current;
    private final ScheduledExecutorService renewer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object closeRelease = new Object();
    /** An explicit close retries its original release until the call acknowledges completion. */
    private boolean closeReleasePending;
    /** What losing the claim stops; null until the capture it guards is open. Guarded by this. */
    private Runnable lost;
    /** Whether the claim was lost while nothing was bound to answer for it yet. Guarded by this. */
    private boolean lostUnanswered;
    private final AtomicReference<BooleanSupplier> needed = new AtomicReference<>(() -> true);

    private CaptureClaimLease() {
        this.ownership = null;
        this.current = new AtomicReference<>();
        this.lost = () -> { };
        this.renewer = null;
        this.closed.set(true);
    }

    static CaptureClaimLease unfenced() {
        return new CaptureClaimLease();
    }

    CaptureClaimLease(
            CaptureOwnership ownership,
            WorkloadClaim initial,
            Duration renewInterval,
            Runnable lost) {
        this(ownership, initial, renewInterval);
        onLost(lost);
    }

    /** Starts renewing {@code initial} now; what losing it stops is bound later with {@link #onLost}. */
    CaptureClaimLease(CaptureOwnership ownership, WorkloadClaim initial, Duration renewInterval) {
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        Objects.requireNonNull(renewInterval, "renewInterval");
        this.renewer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-capture-claim-renewer");
            thread.setDaemon(true);
            return thread;
        });
        renewer.scheduleWithFixedDelay(
                this::renew, renewInterval.toMillis(), renewInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Binds what losing the claim stops, once the capture it guards is open. A claim already lost by then is
     * answered at once, on the caller's thread: the capture was opened over a claim somebody else may hold.
     */
    void onLost(Runnable action) {
        Objects.requireNonNull(action, "action");
        boolean answerNow;
        synchronized (this) {
            lost = action;
            answerNow = lostUnanswered;
            lostUnanswered = false;
        }
        if (answerNow) {
            action.run();
        }
    }

    /** An opened capture outlives its opener only while it still serves a pipeline driven here. */
    void whileNeeded(BooleanSupplier needed) {
        this.needed.set(Objects.requireNonNull(needed, "needed"));
    }

    /** Reserves an attachment before its potentially blocking open, atomically with retiring this lease. */
    synchronized boolean retain(Runnable reserve) {
        if (renewer != null && closed.get()) {
            return false;
        }
        reserve.run();
        return true;
    }

    void renew() {
        if (closed.get()) {
            return;
        }
        try {
            if (!needed.get().getAsBoolean()) {
                lose(true);
                if (closed.get()) {
                    return;
                }
            }
            ownership.renew(current.get()).ifPresentOrElse(
                    current::set,
                    this::lose);
        } catch (RuntimeException unavailable) {
            lose();
        }
    }

    /**
     * Stops the capture, and only then the renewer. This runs on the renewer's own thread, and shutting the
     * renewer down interrupts it: the stop has to be able to wait -- on the capture's thread, the connector,
     * the store -- and with the flag already set the first of those waits returned at once, leaving a
     * fenced-out tail half stopped. Nothing is renewed in between, since {@code closed} is already set.
     */
    private void lose() {
        lose(false);
    }

    private void lose(boolean release) {
        Runnable action;
        synchronized (this) {
            // A new driven attachment may have reserved the tail since the renewer's first look.
            if ((release && needed.get().getAsBoolean()) || !closed.compareAndSet(false, true)) {
                return;
            }
            action = lost;
            lostUnanswered = action == null;
        }
        try {
            try {
                // Release before cleanup waits on an opening connector or another pipeline's coordinator lock.
                if (release) {
                    ownership.release(current.get());
                }
            } finally {
                if (action != null) {
                    action.run();
                }
            }
        } finally {
            renewer.shutdownNow();
        }
    }

    @Override
    public void close() {
        synchronized(closeRelease) {
            if(closed.compareAndSet(false,true))closeReleasePending=ownership!=null;
            if(!closeReleasePending)return;
            if(renewer!=null)renewer.shutdownNow();
            ownership.release(current.get());
            closeReleasePending=false;
        }
    }
}
