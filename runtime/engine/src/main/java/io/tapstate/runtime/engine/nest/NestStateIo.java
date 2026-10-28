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

        private Batcher(boolean writing) {
            this.writing = writing;
        }

        private synchronized Pending pending() {
            return new Pending(pending.size(), pendingKeys, pendingBytes);
        }

        private Map<String, byte[]> call(Map<String, byte[]> values, boolean batchRead) {
            if (values.isEmpty()) return Map.of();
            Request request = new Request(values, batchRead);
            boolean interrupted = false;
            try {
                synchronized (this) {
                    while (!roomFor(request)) interrupted |= waitForChange();
                    pending.add(request);
                    pendingKeys += values.size();
                    pendingBytes += request.bytes;
                }
                while (true) {
                    List<Request> batch;
                    synchronized (this) {
                        if (request.done) {
                            if (request.failure instanceof RuntimeException failure) throw failure;
                            if (request.failure instanceof Error failure) throw failure;
                            return request.result;
                        }
                        if (running) {
                            interrupted |= waitForChange();
                            continue;
                        }
                        running = true;
                        batch = takeBatch();
                        notifyAll();
                    }
                    execute(batch);
                }
            } finally {
                // Abandoning an in-flight write cannot turn an unknown durable outcome into success.
                // Keep the interrupt, but only return after the synchronous backend has answered.
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
            Map<String, byte[]> found = Map.of();
            Throwable failure = null;
            try {
                if (writing) {
                    if (combined.size() == 1) {
                        Map.Entry<String, byte[]> only = combined.entrySet().iterator().next();
                        store.save(namespace, only.getKey(), only.getValue());
                    } else {
                        store.saveAll(namespace, combined);
                    }
                } else if (combined.size() == 1 && batch.stream().noneMatch(request -> request.batchRead)) {
                    String key = combined.keySet().iterator().next();
                    found = store.load(namespace, key).map(value -> Map.of(key, value)).orElse(Map.of());
                } else {
                    found = store.loadAll(namespace, combined.keySet());
                }
            } catch (RuntimeException | Error raised) {
                failure = raised;
            }
            synchronized (this) {
                for (Request request : batch) {
                    Map<String, byte[]> own = new LinkedHashMap<>();
                    if (!writing && failure == null) {
                        for (String key : request.values.keySet()) {
                            byte[] value = found.get(key);
                            if (value != null) own.put(key, value);
                        }
                    }
                    request.result = own;
                    request.failure = failure;
                    request.done = true;
                }
                running = false;
                notifyAll();
            }
        }

        private boolean waitForChange() {
            try {
                wait();
                return false;
            } catch (InterruptedException interrupted) {
                return true;
            }
        }
    }

    private final class Request {
        private final Map<String, byte[]> values;
        private final long bytes;
        private final boolean batchRead;
        private boolean done;
        private Map<String, byte[]> result;
        private Throwable failure;

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
