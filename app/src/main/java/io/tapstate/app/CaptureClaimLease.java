package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    /** What losing the claim stops; null until the capture it guards is open. Guarded by this. */
    private Runnable lost;
    /** Whether the claim was lost while nothing was bound to answer for it yet. Guarded by this. */
    private boolean lostUnanswered;

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

    void renew() {
        if (closed.get()) {
            return;
        }
        try {
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Runnable action;
        synchronized (this) {
            action = lost;
            lostUnanswered = action == null;
        }
        try {
            if (action != null) {
                action.run();
            }
        } finally {
            renewer.shutdownNow();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (renewer != null) {
            renewer.shutdownNow();
        }
        if (ownership != null) {
            ownership.release(current.get());
        }
    }
}
