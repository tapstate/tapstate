package io.tapstate.app;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs periodic observation work without lending it the convergence thread or an unbounded queue. */
final class PeriodicObservations implements AutoCloseable {

    enum Offer { ACCEPTED, COALESCED, REJECTED }

    private final Logger log;
    private final String workName;
    private final ThreadPoolExecutor workers;
    private final Map<String, Slot> slots = new HashMap<>();
    private final ExecutorService healthReporter;
    private final ArrayDeque<Health> pendingHealth = new ArrayDeque<>(2);
    private boolean reportingHealth;
    private volatile boolean closed;
    private boolean backpressured;
    private long coalesced;
    private long rejected;

    record Health(int retainedPipelines, int activeWorkers, int queuedPipelines,
            long coalescedRequests, long rejectedRequests, boolean backpressured) {}

    synchronized Health health() {
        return new Health(slots.size(), workers.getActiveCount(), workers.getQueue().size(),
                coalesced, rejected, backpressured);
    }

    synchronized Offer offer(String pipelineId, Object version, Consumer<BooleanSupplier> task) {
        Offer result = offerLocked(pipelineId, version, task);
        if (!closed && (result == Offer.COALESCED || result == Offer.REJECTED)) {
            if (result == Offer.COALESCED) coalesced++;
            else rejected++;
            enterBackpressure();
        }
        return result;
    }

    private void enterBackpressure() {
        if (backpressured) return;
        backpressured = true;
        queueHealthReport();
    }

    // Edges alternate. While the logger is stalled, replace a superseded pair with its latest edge.
    private void queueHealthReport() {
        if (pendingHealth.size() == 2) pendingHealth.clear();
        pendingHealth.addLast(health());
        if (!reportingHealth) {
            reportingHealth = true;
            healthReporter.execute(this::drainHealthReports);
        }
    }

    private void drainHealthReports() {
        try {
            for (;;) {
                Health snapshot;
                synchronized (this) {
                    snapshot = pendingHealth.pollFirst();
                    if (snapshot == null) {
                        reportingHealth = false;
                        return;
                    }
                }
                reportHealth(snapshot);
            }
        } catch (RuntimeException loggingFailure) {
            synchronized (this) {
                reportingHealth = false;
                if (!closed && !pendingHealth.isEmpty()) {
                    reportingHealth = true;
                    healthReporter.execute(this::drainHealthReports);
                }
            }
            throw loggingFailure;
        }
    }

    private void reportHealth(Health snapshot) {
        if (snapshot.backpressured()) {
            log.warn("{} backpressure: requests are being coalesced or dropped; {}", workName, snapshot);
        } else {
            log.info("{} backpressure cleared; {}", workName, snapshot);
        }
    }

    private static final class Slot {
        private Runnable pending;

        private Object version;
        private final AtomicReference<Object> ticket = new AtomicReference<>();

        private void discard() {
            pending = null;
            version = null;
            ticket.set(null);
        }
    }

    PeriodicObservations() {
        this(4, 64);
    }

    PeriodicObservations(int concurrency, int capacity) {
        this(concurrency, capacity, PeriodicObservations.class, "Periodic observation",
                "tapstate-periodic-observation-");
    }

    PeriodicObservations(int concurrency, int capacity, Class<?> loggerType, String workName, String threadPrefix) {
        this.log = LoggerFactory.getLogger(loggerType);
        this.workName = workName;
        healthReporter = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon(true)
                .name(threadPrefix + "health").factory());
        workers = new ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(capacity), Thread.ofPlatform().daemon(true)
                        .name(threadPrefix, 1).factory());
    }

    private Offer offerLocked(String pipelineId, Object version, Consumer<BooleanSupplier> task) {
        if (closed) {
            return Offer.REJECTED;
        }
        var existing = slots.get(pipelineId);
        var slot = existing == null ? new Slot() : existing;
        boolean replaced = slot.pending != null;
        if (!version.equals(slot.version)) {
            slot.version = version;
            slot.ticket.set(new Object());
        }
        Object ticket = slot.ticket.get();
        slot.pending = () -> task.accept(() -> !closed && ticket == slot.ticket.get());
        if (existing != null) {
            return replaced ? Offer.COALESCED : Offer.ACCEPTED;
        }
        slots.put(pipelineId, slot);
        return enqueue(pipelineId, slot) ? Offer.ACCEPTED : Offer.REJECTED;
    }

    synchronized Set<String> pipelineIds() {
        return Set.copyOf(slots.keySet());
    }

    synchronized void discard(String pipelineId) {
        var slot = slots.get(pipelineId);
        if (slot != null) {
            slot.discard();
        }
    }

    synchronized void retain(Collection<String> ids) {
        Set<String> kept = Set.copyOf(ids);
        slots.forEach((id, slot) -> {
            if (!kept.contains(id)) {
                slot.discard();
            }
        });
    }

    // All submissions, including a follow-up after a worker finishes, happen under the slot lock.
    private boolean enqueue(String pipelineId, Slot slot) {
        try {
            workers.execute(() -> run(pipelineId, slot));
            return true;
        } catch (RejectedExecutionException full) {
            slots.remove(pipelineId);
            return false;
        }
    }

    private void run(String pipelineId, Slot slot) {
        Runnable task;
        synchronized (this) {
            if (closed) {
                return;
            }
            task = slot.pending;
            slot.pending = null;
        }
        try {
            if (task != null) {
                task.run();
            }
        } finally {
            synchronized (this) {
                if (closed || slot.pending == null) {
                    slots.remove(pipelineId);
                } else if (!enqueue(pipelineId, slot)) {
                    // A busy pipeline goes to the back of the queue rather than occupying a worker indefinitely.
                    rejected++;
                    enterBackpressure();
                }
                if (!closed && backpressured && slots.isEmpty()) {
                    backpressured = false;
                    queueHealthReport();
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        slots.clear();
        pendingHealth.clear();
        workers.shutdownNow();
        healthReporter.shutdownNow();
    }

    void awaitStopped(long deadline) {
        ShutdownAwait.executor(workers, deadline);
        ShutdownAwait.executor(healthReporter, deadline);
    }
}
