package io.tapstate.cli;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Coalesces worker publications behind one render-thread callback.
 *
 * <p>Publishing replaces the pending immutable event atomically. The scheduled callback remains
 * claimed while it drains, so publications during delivery coalesce behind it. The release
 * handshake schedules at most one successor when needed, without losing the newest event or
 * queuing a callback per result.
 */
final class LatestOnlyMailbox<E extends WorkbenchEvent> {

    private final Scheduler scheduler;
    private final Consumer<E> ownerThreadConsumer;
    private final Runnable ownershipReleased;
    private final AtomicReference<E> pending = new AtomicReference<>();
    private final AtomicBoolean scheduled = new AtomicBoolean();

    LatestOnlyMailbox(Scheduler scheduler, Consumer<E> ownerThreadConsumer) {
        this(scheduler, ownerThreadConsumer, () -> {
        });
    }

    LatestOnlyMailbox(Scheduler scheduler, Consumer<E> ownerThreadConsumer, Runnable ownershipReleased) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.ownerThreadConsumer = Objects.requireNonNull(ownerThreadConsumer, "ownerThreadConsumer");
        this.ownershipReleased = Objects.requireNonNull(ownershipReleased, "ownershipReleased");
    }

    void publish(E event) {
        pending.set(Objects.requireNonNull(event, "event"));
        scheduleDrain();
    }

    private void scheduleDrain() {
        if (!scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            scheduler.runLater(this::drain);
        } catch (RuntimeException | Error failure) {
            scheduled.set(false);
            reschedulePendingAfter(failure);
            throw failure;
        }
    }

    private void reschedulePendingAfter(Throwable initialFailure) {
        if (pending.get() == null || !scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            scheduler.runLater(this::drain);
        } catch (RuntimeException | Error rescheduleFailure) {
            scheduled.set(false);
            if (rescheduleFailure != initialFailure) {
                initialFailure.addSuppressed(rescheduleFailure);
            }
        }
    }

    private void drain() {
        try {
            E event = pending.getAndSet(null);
            if (event != null) {
                ownerThreadConsumer.accept(event);
            }
        } catch (RuntimeException | Error failure) {
            scheduled.set(false);
            if (pending.get() != null) {
                scheduleDrain();
            }
            throw failure;
        }
        scheduled.set(false);
        ownershipReleased.run();
        if (pending.get() != null) {
            scheduleDrain();
        }
    }

    /** The scheduling seam implemented by {@code TuiRunner::runLater} in the live workbench. */
    @FunctionalInterface
    interface Scheduler {
        void runLater(Runnable callback);
    }
}
