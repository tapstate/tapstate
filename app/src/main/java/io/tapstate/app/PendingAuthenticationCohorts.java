package io.tapstate.app;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Shares validation only among calls admitted before that validation starts. Never caches a result. */
final class PendingAuthenticationCohorts<T> implements AutoCloseable {
    private static final int MAX_CREDENTIAL_LENGTH = 8192;
    private final long gatheringNanos;
    private final int maxGroups;
    private final int maxMembers;
    private final Map<Key, Group<T>> open = new HashMap<>();
    private final Set<Group<T>> active = new HashSet<>();
    private boolean closed;

    PendingAuthenticationCohorts() { this(Duration.ofMillis(20), 128, 32); }

    PendingAuthenticationCohorts(Duration gathering, int maxGroups, int maxMembers) {
        if (gathering.isNegative() || maxGroups < 1 || maxMembers < 1) throw new IllegalArgumentException();
        this.gatheringNanos = gathering.toNanos(); this.maxGroups = maxGroups; this.maxMembers = maxMembers;
    }

    T validate(Object epoch, String credential, long deadline, Supplier<T> validation, Runnable localFence) {
        remaining(deadline); localFence.run();
        Group<T> group = null; boolean leader = false;
        synchronized (this) {
            if (closed) throw StorageRuntime.unavailable();
            if (credential != null && credential.length() <= MAX_CREDENTIAL_LENGTH) {
                var key = new Key(epoch, credential);
                group = open.get(key);
                if (group != null && group.members < maxMembers) {
                    group.members++; notifyAll();
                } else if (active.size() < maxGroups) {
                    group = new Group<>(key, System.nanoTime() + gatheringNanos);
                    open.put(key, group); active.add(group); leader = true;
                } else group = null;
            }
        }
        if (group == null) {
            // Saturation keeps the original per-call validation and deadline.
            remaining(deadline); T value = validation.get();
            remaining(deadline); localFence.run(); return value;
        }
        if (leader) {
            try {
                synchronized (this) {
                    while (!group.result.isDone() && group.members < maxMembers) {
                        long left = Math.min(remaining(deadline), group.sealAt - System.nanoTime());
                        if (left <= 0) break;
                        TimeUnit.NANOSECONDS.timedWait(this, left);
                    }
                    // Remove atomically before any durable read. Later calls cannot join this result.
                    open.remove(group.key, group);
                }
                if (!group.result.isDone()) {
                    remaining(deadline); localFence.run();
                    T value = validation.get();
                    remaining(deadline); localFence.run();
                    group.result.complete(value);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); group.result.completeExceptionally(StorageRuntime.unavailable());
            } catch (RuntimeException | Error failure) {
                group.result.completeExceptionally(failure);
            } finally {
                synchronized (this) { open.remove(group.key, group); active.remove(group); }
            }
        }
        try {
            T value = group.result.get(remaining(deadline), TimeUnit.NANOSECONDS);
            remaining(deadline); localFence.run(); return value;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw StorageRuntime.unavailable();
        } catch (TimeoutException expired) {
            throw StorageRuntime.unavailable();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException failure) throw failure;
            if (failed.getCause() instanceof Error failure) throw failure;
            throw new IllegalStateException("unexpected authentication validation failure", failed.getCause());
        }
    }

    synchronized void invalidate() { invalidate(StorageRuntime.unavailable()); }

    synchronized void invalidate(RuntimeException reason) {
        java.util.Objects.requireNonNull(reason, "reason");
        for (var group : active) group.result.completeExceptionally(reason);
        open.clear(); notifyAll();
    }

    @Override public synchronized void close() { closed = true; invalidate(); }

    private static long remaining(long deadline) {
        long left = deadline - System.nanoTime();
        if (left <= 0 || Thread.currentThread().isInterrupted()) throw StorageRuntime.unavailable();
        return left;
    }

    private static final class Group<T> {
        final Key key;
        final long sealAt;
        final CompletableFuture<T> result = new CompletableFuture<>();
        int members = 1;
        Group(Key key, long sealAt) { this.key = key; this.sealAt = sealAt; }
    }

    private static final class Key {
        private final Object epoch;
        private final String credential;
        Key(Object epoch, String credential) { this.epoch = epoch; this.credential = credential; }
        @Override public boolean equals(Object other) {
            return other instanceof Key key && epoch == key.epoch && credential.equals(key.credential);
        }
        @Override public int hashCode() { return 31 * System.identityHashCode(epoch) + credential.hashCode(); }
        @Override public String toString() { return "PendingAuthenticationKey[redacted]"; }
    }
}
