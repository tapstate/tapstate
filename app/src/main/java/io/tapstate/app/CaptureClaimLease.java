package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Renews one capture claim and stops its local capture immediately when ownership can no longer be proved. */
final class CaptureClaimLease implements AutoCloseable {

    private final CaptureOwnership ownership;
    private final AtomicReference<WorkloadClaim> current;
    private final Runnable lost;
    private final ScheduledExecutorService renewer;
    private final AtomicBoolean closed = new AtomicBoolean();

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
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        this.lost = Objects.requireNonNull(lost, "lost");
        Objects.requireNonNull(renewInterval, "renewInterval");
        this.renewer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-capture-claim-renewer");
            thread.setDaemon(true);
            return thread;
        });
        renewer.scheduleWithFixedDelay(
                this::renew, renewInterval.toMillis(), renewInterval.toMillis(), TimeUnit.MILLISECONDS);
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
        try {
            lost.run();
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
