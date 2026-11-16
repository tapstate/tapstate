package io.tapstate.runtime.engine.nest;

import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.StoreRoundTrips;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Coalesces partition callbacks for the same namespace's backend. Selected root bulk reads and
 * fresh writes gather concurrent callbacks against independent one-millisecond deadlines.
 * No executor, cache or background write is introduced. Every caller still waits for its own
 * durable backend outcome, preserving write-through and split-brain protection.
 *
 * <p>Hazelcast splits getAll/setAll by partition. Batching only above the map therefore still sends a
 * remote command per partition. Combining the pending calls here keeps resident hits in the map and
 * amortizes remote I/O without bypassing it. Backend responses are returned only to their own callers.
 */
final class NestStateIo {
    static final int MAX_KEYS = 1_000;
    static final long MAX_BYTES = 8L * 1024 * 1024;
    static final int MAX_REQUESTS = 64;

    private final KeyedStateStore store;
    private final String namespace;
    private final long writeGatherNanos;
    private final long readGatherNanos;
    private final BooleanSupplier rootBulkReads;
    private final LongSupplier readNanos;
    // Volatile publishes the lazy batcher. The batcher protects its queue with its own lock.
    @SuppressWarnings("java:S3077")
    private volatile Batcher reads;
    private final Batcher writes = new Batcher(true);

    private final Committed committed;

    NestStateIo(KeyedStateStore store, String namespace) {
        this(store, namespace, null, Committed.NONE);
    }

    NestStateIo(KeyedStateStore store, String namespace, BooleanSupplier rootBulkReads) {
        this(store, namespace, rootBulkReads, Committed.NONE);
    }

    NestStateIo(KeyedStateStore store, String namespace, BooleanSupplier rootBulkReads, Committed committed) {
        this(store, namespace, 1_000_000L, rootBulkReads == null ? 0L : 1_000_000L,
                rootBulkReads == null ? () -> false : rootBulkReads, System::nanoTime, committed);
    }

    /** Bounded timing seams for concurrency tests; production windows remain fixed at one millisecond. */
    NestStateIo(KeyedStateStore store, String namespace, long writeGatherNanos) {
        this(store, namespace, writeGatherNanos, 0);
    }

    NestStateIo(KeyedStateStore store, String namespace, long writeGatherNanos, long readGatherNanos) {
        this(store, namespace, writeGatherNanos, readGatherNanos, () -> true);
    }

    NestStateIo(KeyedStateStore store, String namespace, long writeGatherNanos,
            long readGatherNanos, BooleanSupplier rootBulkReads) {
        this(store, namespace, writeGatherNanos, readGatherNanos, rootBulkReads, System::nanoTime);
    }

    NestStateIo(KeyedStateStore store, String namespace, long writeGatherNanos,
            long readGatherNanos, BooleanSupplier rootBulkReads, LongSupplier readNanos) {
        this(store, namespace, writeGatherNanos, readGatherNanos, rootBulkReads, readNanos, Committed.NONE);
    }

    private NestStateIo(KeyedStateStore store, String namespace, long writeGatherNanos,
            long readGatherNanos, BooleanSupplier rootBulkReads, LongSupplier readNanos, Committed committed) {
        if (writeGatherNanos < 0 || writeGatherNanos > 1_000_000_000L
                || readGatherNanos < 0 || readGatherNanos > 1_000_000_000L)
            throw new IllegalArgumentException("state gather duration is outside its bounded range");
        this.writeGatherNanos = writeGatherNanos;
        this.readGatherNanos = readGatherNanos;
        this.rootBulkReads = Objects.requireNonNull(rootBulkReads, "rootBulkReads");
        this.readNanos = Objects.requireNonNull(readNanos, "readNanos");
        this.store = Objects.requireNonNull(store, "store");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.committed = Objects.requireNonNull(committed, "committed");
    }

    /**
     * Told of every commit group once the store has made it durable: how many keys it carried, how many
     * store round trips it cost, and how long it took from issue to acknowledgement. A group that failed is
     * not a commit and is not reported.
     */
    @FunctionalInterface
    interface Committed {
        Committed NONE = (keys, roundTrips, nanos) -> { };

        void group(long keys, long roundTrips, long nanos);
    }

    /** Runs one store write as one commit group, reporting it once it succeeded. */
    static void commit(Committed committed, long keys, Runnable write) {
        if (committed == Committed.NONE) {
            write.run();
            return;
        }
        long began = System.nanoTime();
        long roundTrips;
        try (StoreRoundTrips.Count count = StoreRoundTrips.count()) {
            write.run();
            roundTrips = count.roundTrips();
        }
        committed.group(keys, roundTrips, System.nanoTime() - began);
    }

    Map<String, byte[]> loadAll(Collection<String> keys) {
        Map<String, byte[]> request = new LinkedHashMap<>();
        for (String key : keys) request.put(key, null);
        if (request.isEmpty()) return Map.of();
        boolean gatherRead = readGatherNanos != 0 && rootBulkReads.getAsBoolean();
        return readBatcher(gatherRead).call(request, true, gatherRead);
    }

    byte[] load(String key) {
        Map<String, byte[]> request = new LinkedHashMap<>();
        request.put(key, null);
        Batcher reader = reads;
        if (reader == null) reader = readBatcher(readGatherNanos != 0 && rootBulkReads.getAsBoolean());
        return reader.call(request, false, false).get(key);
    }

    void saveAll(Map<String, byte[]> states) {
        writes.call(new LinkedHashMap<>(states), false, false);
    }

    /** Admission facts used to verify that blocked callers cannot grow the internal queue without bound. */
    Pending queuedReads() {
        Batcher reader = reads;
        return reader == null ? new Pending(0, 0, 0) : reader.pending();
    }
    Pending queuedWrites() { return writes.pending(); }
    record Pending(int requests, int keys, long bytes) { }

    /** Role classification happens before this monitor; it never replaces the backend's active guards. */
    private Batcher readBatcher(boolean rootState) {
        Batcher reader = reads;
        if (reader == null) {
            synchronized (this) {
                reader = reads;
                if (reader == null) reads = reader = new Batcher(false, rootState);
            }
        }
        return reader;
    }

    private final class Batcher {
        private final boolean writing;
        private final Queue<Request> pending = new ArrayDeque<>();
        private int pendingKeys;
        private long pendingBytes;
        private boolean running;
        private int admissionWaiters;
        private int isolatedWrites;
        private int isolatedBulkReads;
        private final ReentrantLock lock;
        private final Condition changed;
        private final Condition arrivals;

        private Batcher(boolean writing) {
            this(writing, false);
        }

        private Batcher(boolean writing, boolean fair) {
            this.writing = writing;
            this.lock = new ReentrantLock(fair);
            this.changed = lock.newCondition();
            this.arrivals = lock.newCondition();
        }

        private Pending pending() {
            lock.lock();
            try {
                return new Pending(pending.size(), pendingKeys, pendingBytes);
            } finally {
                lock.unlock();
            }
        }

        private Map<String, byte[]> call(Map<String, byte[]> values, boolean batchRead, boolean gatherRead) {
            if (values.isEmpty()) return Map.of();
            Request request = new Request(values, batchRead, gatherRead);
            boolean interrupted = false;
            try {
                lock.lock();
                try {
                    while (!roomFor(request)) {
                        admissionWaiters++;
                        arrivals.signal();
                        try {
                            changed.awaitUninterruptibly();
                            interrupted |= Thread.interrupted();
                        } finally {
                            admissionWaiters--;
                        }
                    }
                    request.admittedAt = System.nanoTime();
                    pending.add(request);
                    pendingKeys += values.size();
                    pendingBytes += request.bytes;
                    if (writing || readGatherNanos != 0) arrivals.signal();
                } finally {
                    lock.unlock();
                }
                while (true) {
                    List<Request> batch;
                    lock.lock();
                    try {
                        if (request.done) return request.answer();
                        if (running) {
                            changed.awaitUninterruptibly();
                            interrupted |= Thread.interrupted();
                            continue;
                        }
                        running = true;
                        interrupted |= gatherWrites();
                        if (!writing) interrupted |= gatherReads();
                        batch = takeBatch();
                        changed.signalAll();
                    } finally {
                        lock.unlock();
                    }
                    execute(batch);
                }
            } finally {
                // Defer interruption until the shared synchronous outcome is known; no write can be
                // acknowledged merely because its waiter stopped waiting. Preserve the caller's flag.
                if (interrupted) Thread.currentThread().interrupt();
            }
        }

        // Both gatherers recompute the original deadline and let submit restore the interrupt after completion.
        @SuppressWarnings({"java:S899", "java:S2142"})
        private boolean gatherWrites() {
            if (!writing || writeGatherNanos == 0 || isolatedWrites >= 2) return false;
            boolean interrupted = false;
            long began = pending.element().admittedAt;
            // The oldest queued request owns the deadline. Arrivals and interrupts never extend it;
            // requests that already waited through backend I/O need no additional gathering delay.
            while (canGather()) {
                long remaining = writeGatherNanos - (System.nanoTime() - began);
                if (remaining <= 0) break;
                try {
                    arrivals.awaitNanos(remaining);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            return interrupted;
        }

        @SuppressWarnings({"java:S899", "java:S2142"})
        private boolean gatherReads() {
            if (readGatherNanos == 0 || isolatedBulkReads >= 2 || !pending.element().gatherRead) return false;
            boolean interrupted = false;
            boolean awaitAttempted = false;
            long began = pending.element().admittedAt;
            while (admissionWaiters == 0 && pending.size() < MAX_REQUESTS
                    && pendingKeys < MAX_KEYS && pendingBytes < MAX_BYTES
                    && pending.stream().allMatch(request -> request.gatherRead)) {
                long remaining = readGatherNanos - (readNanos.getAsLong() - began);
                if (remaining <= 0) {
                    // Give existing admission predecessors one turn if no timed wait released this lock.
                    // The fair root read queue keeps this leader behind them without opening a new window.
                    if (!awaitAttempted && lock.isFair() && lock.hasQueuedThreads()) {
                        lock.unlock();
                        lock.lock();
                        interrupted |= Thread.interrupted();
                    }
                    break;
                }
                awaitAttempted = true;
                try {
                    arrivals.awaitNanos(remaining);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            return interrupted;
        }

        private boolean canGather() {
            if (admissionWaiters != 0 || pending.size() >= MAX_REQUESTS
                    || pendingKeys >= MAX_KEYS || pendingBytes >= MAX_BYTES) return false;
            Set<String> keys = new HashSet<>();
            for (Request request : pending) {
                for (String key : request.values.keySet()) if (!keys.add(key)) return false;
            }
            return true;
        }

        private boolean roomFor(Request request) {
            // A single oversized request keeps its existing backend semantics and is never combined.
            return pending.isEmpty() || (pending.size() < MAX_REQUESTS
                    && request.values.size() <= MAX_KEYS - pendingKeys
                    && request.bytes <= MAX_BYTES - pendingBytes);
        }

        private List<Request> takeBatch() {
            List<Request> batch = new ArrayList<>();
            Map<String, byte[]> keys = new LinkedHashMap<>();
            while (!pending.isEmpty()) {
                Request next = pending.peek();
                // Do not collapse competing writes of the same key into one successful replacement.
                if (writing && next.values.keySet().stream().anyMatch(keys::containsKey)) break;
                pending.remove();
                pendingKeys -= next.values.size();
                pendingBytes -= next.bytes;
                batch.add(next);
                keys.putAll(next.values);
            }
            return batch;
        }

        private void execute(List<Request> batch) {
            Map<String, byte[]> combined = new LinkedHashMap<>();
            batch.forEach(request -> combined.putAll(request.values));
            boolean batchRead = batch.stream().anyMatch(request -> request.batchRead);
            // Run on this caller, not an executor. FutureTask captures even fatal failures so every
            // waiter gets the same outcome before the original failure is rethrown by its caller.
            FutureTask<Map<String, byte[]>> outcome = new FutureTask<>(() -> backend(combined, batchRead));
            outcome.run();
            Throwable failure = outcome.state() == Future.State.FAILED ? outcome.exceptionNow() : null;
            Map<String, byte[]> found = failure == null ? outcome.resultNow() : Map.of();
            lock.lock();
            try {
                for (Request request : batch) {
                    request.result = subset(found, request.values.keySet());
                    request.failure = failure;
                    request.done = true;
                }
                // A serial partition or sparse stream cannot benefit from waiting. Stop paying the
                // gather window after two isolated writes, and relearn when concurrent work appears.
                if (writing) isolatedWrites = batch.size() > 1 || !pending.isEmpty()
                        ? 0 : Math.min(2, isolatedWrites + 1);
                if (!writing && batch.stream().anyMatch(request -> request.gatherRead)) {
                    isolatedBulkReads = batch.size() > 1 || !pending.isEmpty()
                            ? 0 : Math.min(2, isolatedBulkReads + 1);
                }
                running = false;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }

        private Map<String, byte[]> backend(Map<String, byte[]> combined, boolean batchRead) {
            if (writing) {
                write(combined);
                return Map.of();
            }
            if (combined.size() == 1 && !batchRead) {
                String key = combined.keySet().iterator().next();
                return store.load(namespace, key).map(value -> Map.of(key, value)).orElse(Map.of());
            }
            return Objects.requireNonNull(store.loadAll(namespace, combined.keySet()), "loaded states");
        }

        private void write(Map<String, byte[]> combined) {
            commit(committed, combined.size(), () -> {
                if (combined.size() == 1) {
                    Map.Entry<String, byte[]> only = combined.entrySet().iterator().next();
                    store.save(namespace, only.getKey(), only.getValue());
                } else {
                    store.saveAll(namespace, combined);
                }
            });
        }
    }

    private static Map<String, byte[]> subset(Map<String, byte[]> found, Collection<String> keys) {
        Map<String, byte[]> own = new LinkedHashMap<>();
        for (String key : keys) {
            byte[] value = found.get(key);
            if (value != null) own.put(key, value);
        }
        return own;
    }

    private final class Request {
        private final Map<String, byte[]> values;
        private final long bytes;
        private final boolean batchRead;
        private final boolean gatherRead;
        private long admittedAt;
        private boolean done;
        private Map<String, byte[]> result;
        private Throwable failure;

        private Map<String, byte[]> answer() {
            if (failure instanceof RuntimeException raised) throw raised;
            if (failure instanceof Error raised) throw raised;
            if (failure != null) throw new IllegalStateException("unexpected checked state I/O failure", failure);
            return result;
        }

        private Request(Map<String, byte[]> values, boolean batchRead, boolean gatherRead) {
            this.values = values;
            this.batchRead = batchRead;
            this.gatherRead = gatherRead;
            long size = 0;
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                size += 64L + 3L * (namespace.length() + entry.getKey().length());
                if (entry.getValue() != null) size += entry.getValue().length;
            }
            this.bytes = size;
        }
    }
}
