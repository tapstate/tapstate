package io.tapstate.runtime.srs;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** A fixed data-plane pool whose capacity is reserved before a snapshot job is submitted. */
public final class SnapshotWorkers implements AutoCloseable {

    public static final int DEFAULT_CONCURRENCY = 4;
    public static final int DEFAULT_QUEUE_CAPACITY = 64;

    private final Semaphore capacity;
    private final ThreadPoolExecutor workers;
    private final Set<Reservation> reservations = ConcurrentHashMap.newKeySet();

    public SnapshotWorkers(int concurrency, int queueCapacity) {
        if (concurrency < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("snapshot worker and queue budgets must be positive");
        }
        capacity = new Semaphore(Math.addExact(concurrency, queueCapacity));
        AtomicInteger next = new AtomicInteger();
        workers = new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), task -> {
                    Thread thread = new Thread(task, "tapstate-snapshot-" + next.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    /** Returns empty at the fixed capacity without queuing an unbounded future. */
    public Optional<Reservation> reserve() {
        if (!capacity.tryAcquire()) {
            return Optional.empty();
        }
        Reservation reserved = new Reservation();
        reservations.add(reserved);
        return Optional.of(reserved);
    }

    public final class Reservation implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();
        private volatile Work work;
        private volatile boolean closed;

        private Reservation() {
        }

        /** Begins the reserved read only after its Jet consumer has been submitted. */
        public synchronized void activate(Runnable read) {
            if (closed || work != null) {
                throw new IllegalStateException("snapshot reservation was closed or already activated");
            }
            Work started = new Work(this, read);
            work = started;
            try {
                workers.execute(started);
            } catch (java.util.concurrent.RejectedExecutionException stopped) {
                closed = true;
                release();
                throw stopped;
            }
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            Work started = work;
            if (started == null || workers.remove(started)) {
                release();
                return;
            }
            Thread runner = started.runner;
            if (runner != null) {
                runner.interrupt();
            }
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                reservations.remove(this);
                capacity.release();
            }
        }
    }

    private static final class Work implements Runnable {
        private final Reservation reservation;
        private final Runnable read;
        private volatile Thread runner;

        private Work(Reservation reservation, Runnable read) {
            this.reservation = reservation;
            this.read = read;
        }

        @Override
        public void run() {
            runner = Thread.currentThread();
            try {
                if (!reservation.closed) {
                    read.run();
                }
            } finally {
                runner = null;
                reservation.release();
            }
        }
    }

    @Override
    public void close() {
        for (Reservation reservation : Set.copyOf(reservations)) {
            reservation.close();
        }
        workers.shutdownNow();
        try {
            workers.awaitTermination(Duration.ofSeconds(5).toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
