package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.Subscription;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * One capture run's initial load, read on a thread of its own, and the tail that has to wait for it.
 *
 * <p><b>Its own thread because the load waits for the job and the job waits for the start.</b> Rows go
 * into a hand-off that holds a few thousand of them, and room is made only by the job taking them; the job
 * is submitted once the start has returned. A load read on the thread that starts the pipeline would fill
 * the hand-off and then wait for a job that is only submitted once it returns. That thread is also the one
 * every pipeline on the member is converged on, so a load it did read to the end -- hours of it, behind a
 * slow step -- would hold every other pipeline's start and stop for as long.
 *
 * <p><b>The tail opens after the load, as it always has.</b> A change streamed straight to the pipeline goes
 * through the same hand-off as the load, and one handed over ahead of a snapshot row of the same key would be
 * overwritten by the older value; opening the tail only once the last row is in keeps every change behind
 * every snapshot row, on the ring and off it.
 *
 * <p>A failure of either half is recorded on the run's health rather than thrown -- the start that began the
 * run returned long ago -- and is what turns the pipeline failed. An abandoned run records nothing: closing
 * it is somebody asking for it to stop, and what the load does on the way out is the stop, not a fault.
 */
final class BackgroundLoad {

    /**
     * How long an abandonment waits for the reading thread to notice. It is woken rather than waited out --
     * the batch it reads is closed under it and the wait for room is interrupted -- so this bounds only a
     * source that does not answer a stop, which is left to finish on its own and hand over nothing.
     */
    private static final long ABANDON_WAIT_MILLIS = 10_000;

    private final SnapshotPhase.Load load;
    private final CaptureHandoff handoff;
    private final Supplier<Optional<Subscription>> tail;
    private final CaptureHealth health;
    private final AtomicLong rows = new AtomicLong();
    private final Map<String, Long> rowsByTable = new ConcurrentHashMap<>();
    private final CountDownLatch finished = new CountDownLatch(1);
    private final Thread thread;

    /** Set once, by an abandonment; guarded by this. */
    private boolean cancelled;

    /** The tail once it has opened, until the run is closed; guarded by this. */
    private Subscription opened;

    BackgroundLoad(SnapshotPhase.Load load, CaptureHandoff handoff, Supplier<Optional<Subscription>> tail,
            CaptureHealth health, String threadName) {
        this.load = Objects.requireNonNull(load, "load");
        this.handoff = Objects.requireNonNull(handoff, "handoff");
        this.tail = Objects.requireNonNull(tail, "tail");
        this.health = Objects.requireNonNull(health, "health");
        this.thread = new Thread(this::run, Objects.requireNonNull(threadName, "threadName"));
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    long rows() {
        return rows.get();
    }

    Map<String, Long> rowsByTable() {
        return Map.copyOf(rowsByTable);
    }

    synchronized Optional<Subscription> tail() {
        return Optional.ofNullable(opened);
    }

    boolean finished() {
        return finished.getCount() == 0;
    }

    boolean awaitFinished(Duration timeout) throws InterruptedException {
        return finished.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Abandons the run: stops the load where it is, keeps the tail from opening, and closes it if it
     * already had. Returns once the reading thread has let go, or once it has been given its chance to.
     */
    void cancel() {
        synchronized (this) {
            cancelled = true;
        }
        // The batch being read is closed under the reader and a wait for room is interrupted: those are the
        // two places the thread can be parked for a long time, and neither ends by itself.
        load.close();
        if (Thread.currentThread() != thread) {
            // Closed from outside. Closed from the reading thread itself -- by something it calls on the way
            // -- there is nothing to wake and nobody else to wait for: it sees the abandonment as it returns.
            thread.interrupt();
            try {
                thread.join(ABANDON_WAIT_MILLIS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        Subscription subscription;
        synchronized (this) {
            subscription = opened;
            opened = null;
        }
        if (subscription != null) {
            subscription.close();
        }
    }

    private void run() {
        try {
            load.read(this::pass, this::loaded);
            Optional<Subscription> subscription = tail.get();
            boolean keep;
            synchronized (this) {
                keep = !cancelled;
                if (keep) {
                    opened = subscription.orElse(null);
                }
            }
            if (!keep) {
                // Abandoned while the tail was opening: nobody is left to close it but this thread.
                subscription.ifPresent(Subscription::close);
            }
        } catch (CancellationException abandoned) {
            // Closed while it ran. What was cut short is the stop somebody asked for, not a failure.
        } catch (RuntimeException | Error failure) {
            if (!isCancelled()) {
                health.fail(failure);
            }
        } finally {
            load.close();
            finished.countDown();
        }
    }

    private void pass(Envelope row) {
        if (isCancelled()) {
            throw new CancellationException("the run was closed while its load was being read");
        }
        // Handed over first and counted after: the hand-off may wait for room and be abandoned while it
        // waits, and a row that never went anywhere is not one this run passed on.
        handoff.accept(row);
        rowsByTable.merge(row.src(), 1L, Long::sum);
        rows.incrementAndGet();
        health.received(row);
    }

    private void loaded(String table) {
        if (isCancelled()) {
            throw new CancellationException("the run was closed while its load was being read");
        }
        handoff.loaded(table);
    }

    private synchronized boolean isCancelled() {
        return cancelled;
    }
}
