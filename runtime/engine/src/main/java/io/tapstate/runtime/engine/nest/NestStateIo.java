package io.tapstate.runtime.engine.nest;

import io.tapstate.spi.store.KeyedStateStore;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coalesces partition loaders already waiting for the same namespace's backend. The first caller
 * executes immediately; callers arriving during that I/O form the next bounded batch. No timer,
 * executor, read cache or background write is introduced. Each map operation remains blocked until
 * its own backend result is known, preserving the map's write-through and split-brain protection.
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
    private final Batcher reads = new Batcher(false);
    private final Batcher writes = new Batcher(true);

    NestStateIo(KeyedStateStore store, String namespace) {
        this.store = Objects.requireNonNull(store, "store");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    Map<String, byte[]> loadAll(Collection<String> keys) {
        Map<String, byte[]> request = new LinkedHashMap<>();
        for (String key : keys) request.put(key, null);
        return reads.call(request, true);
    }

    byte[] load(String key) {
        Map<String, byte[]> request = new LinkedHashMap<>();
        request.put(key, null);
        return reads.call(request, false).get(key);
    }

    void saveAll(Map<String, byte[]> states) {
        writes.call(new LinkedHashMap<>(states), false);
    }

    /** Admission facts used to verify that blocked callers cannot grow the internal queue without bound. */
    Pending queuedReads() { return reads.pending(); }
    Pending queuedWrites() { return writes.pending(); }
    record Pending(int requests, int keys, long bytes) { }

    private final class Batcher {
        private final boolean writing;
        private final Queue<Request> pending = new ArrayDeque<>();
        private int pendingKeys;
        private long pendingBytes;
        private boolean running;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();

        private Batcher(boolean writing) {
            this.writing = writing;
        }

        private Pending pending() {
            lock.lock();
            try {
                return new Pending(pending.size(), pendingKeys, pendingBytes);
            } finally {
                lock.unlock();
            }
        }

        private Map<String, byte[]> call(Map<String, byte[]> values, boolean batchRead) {
            if (values.isEmpty()) return Map.of();
            Request request = new Request(values, batchRead);
            boolean interrupted = false;
            try {
                lock.lock();
                try {
                    while (!roomFor(request)) {
                        changed.awaitUninterruptibly();
                        interrupted |= Thread.interrupted();
                    }
                    pending.add(request);
                    pendingKeys += values.size();
                    pendingBytes += request.bytes;
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
            if (combined.size() == 1) {
                Map.Entry<String, byte[]> only = combined.entrySet().iterator().next();
                store.save(namespace, only.getKey(), only.getValue());
            } else {
                store.saveAll(namespace, combined);
            }
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
        private boolean done;
        private Map<String, byte[]> result;
        private Throwable failure;

        private Map<String, byte[]> answer() {
            if (failure instanceof RuntimeException raised) throw raised;
            if (failure instanceof Error raised) throw raised;
            if (failure != null) throw new IllegalStateException("unexpected checked state I/O failure", failure);
            return result;
        }

        private Request(Map<String, byte[]> values, boolean batchRead) {
            this.values = values;
            this.batchRead = batchRead;
            long size = 0;
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                size += 64L + 3L * (namespace.length() + entry.getKey().length());
                if (entry.getValue() != null) size += entry.getValue().length;
            }
            this.bytes = size;
        }
    }
}
