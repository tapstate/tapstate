package io.tapstate.app;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keeps lifecycle work single-flight per Pipeline and off the scheduling thread. */
final class PipelineConvergences implements AutoCloseable {

    enum Offer { ACCEPTED, COALESCED, REJECTED }

    private static final Logger LOG = LoggerFactory.getLogger(PipelineConvergences.class);
    private final ThreadPoolExecutor workers;
    private final Map<String, Slot> slots = new HashMap<>();
    private final Object healthReports = new Object();
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

    Offer offer(String pipelineId, Runnable task) {
        synchronized (healthReports) {
            Offer result;
            Health report = null;
            synchronized (this) {
                result = offerLocked(pipelineId, task);
                if (!closed && (result == Offer.COALESCED || result == Offer.REJECTED)) {
                    if (result == Offer.COALESCED) coalesced++;
                    else rejected++;
                    report = enterBackpressure();
                }
            }
            reportHealth(report);
            return result;
        }
    }

    private Health enterBackpressure() {
        if (backpressured) return null;
        backpressured = true;
        return health();
    }

    private static void reportHealth(Health snapshot) {
        if (snapshot == null) return;
        if (snapshot.backpressured()) {
            LOG.warn("Lifecycle convergence backpressure: requests are being coalesced or dropped; {}", snapshot);
        } else {
            LOG.info("Lifecycle convergence backpressure cleared; {}", snapshot);
        }
    }

    private static final class Slot {
        private Runnable pending;

        private void discard() {
            pending = null;
        }
    }

    PipelineConvergences() {
        this(4, 64);
    }

    PipelineConvergences(int concurrency, int capacity) {
        workers = new ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(capacity), Thread.ofPlatform().daemon(true)
                        .name("tapstate-pipeline-convergence-", 1).factory());
    }

    private synchronized Offer offerLocked(String pipelineId, Runnable task) {
        if (closed) {
            return Offer.REJECTED;
        }
        var existing = slots.get(pipelineId);
        var slot = existing == null ? new Slot() : existing;
        boolean replaced = slot.pending != null;
        slot.pending = task;
        if (existing != null) {
            return replaced ? Offer.COALESCED : Offer.ACCEPTED;
        }
        slots.put(pipelineId, slot);
        return enqueue(pipelineId, slot) ? Offer.ACCEPTED : Offer.REJECTED;
    }

    synchronized Set<String> pipelineIds() {
        return Set.copyOf(slots.keySet());
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
            // Sequence edge reports without holding the slot monitor during logging.
            synchronized (healthReports) {
                Health report = null;
                synchronized (this) {
                    if (closed || slot.pending == null) {
                        slots.remove(pipelineId);
                    } else {
                        // A busy pipeline goes to the back of the queue rather than occupying a worker indefinitely.
                        if (!enqueue(pipelineId, slot)) {
                            rejected++;
                            report = enterBackpressure();
                        }
                    }
                    if (!closed && backpressured && slots.isEmpty()) {
                        backpressured = false;
                        report = health();
                    }
                }
                reportHealth(report);
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        slots.clear();
        workers.shutdownNow();
    }
}
