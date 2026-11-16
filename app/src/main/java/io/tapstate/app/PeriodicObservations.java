package io.tapstate.app;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs periodic observation work without lending it the convergence thread or an unbounded queue. */
final class PeriodicObservations implements AutoCloseable {

    enum Offer { ACCEPTED, COALESCED, REJECTED }

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicObservations.class);
    private final ThreadPoolExecutor workers;
    private final Map<String, Slot> slots = new HashMap<>();
    private volatile boolean closed;

    private static final class Slot {
        private Runnable pending;

        private Object version;
        private volatile Object ticket;

        private void discard() {
            pending = null;
            version = null;
            ticket = null;
        }
    }

    PeriodicObservations() {
        this(4, 64);
    }

    PeriodicObservations(int concurrency, int capacity) {
        workers = new ThreadPoolExecutor(concurrency, concurrency, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(capacity), Thread.ofPlatform().daemon(true)
                        .name("tapstate-periodic-observation-", 1).factory());
    }

    synchronized Offer offer(String pipelineId, Object version, Consumer<BooleanSupplier> task) {
        if (closed) {
            return Offer.REJECTED;
        }
        var existing = slots.get(pipelineId);
        var slot = existing == null ? new Slot() : existing;
        boolean replaced = slot.pending != null;
        if (!version.equals(slot.version)) {
            slot.version = version;
            slot.ticket = new Object();
        }
        Object ticket = slot.ticket;
        slot.pending = () -> task.accept(() -> !closed && ticket == slot.ticket);
        if (existing != null) {
            return replaced ? Offer.COALESCED : Offer.ACCEPTED;
        }
        slots.put(pipelineId, slot);
        return enqueue(pipelineId, slot) ? Offer.ACCEPTED : Offer.REJECTED;
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
            boolean discarded = false;
            synchronized (this) {
                if (closed || slot.pending == null) {
                    slots.remove(pipelineId);
                } else {
                    // A busy pipeline goes to the back of the queue rather than occupying a worker indefinitely.
                    discarded = !enqueue(pipelineId, slot);
                }
            }
            if (discarded) {
                LOG.debug("Periodic observation queue full; dropped a follow-up for pipeline {}", pipelineId);
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
