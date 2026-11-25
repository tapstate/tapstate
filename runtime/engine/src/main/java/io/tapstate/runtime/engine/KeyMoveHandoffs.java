package io.tapstate.runtime.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Admission for one ordered producer and one execution of a conflict domain. Keys must include the
 * identity of their owning state or physical target. Each side reports once all its effects have
 * entered ordered writer queues, then once all its target writes have succeeded. These notices are
 * local execution facts; neither can advance a source recovery position.
 */
public final class KeyMoveHandoffs<K> {
    public interface Receipt {
        /** Whether earlier conflicting target writes have settled on every writer. */
        default boolean mayWrite() { return true; }
        void queued();
        void written();
    }

    private final int capacity;
    private final Set<Input> live = new HashSet<>();
    private final Map<K, Set<Input>> pending = new HashMap<>();
    private final Map<K, Input> orderedTails = new HashMap<>();
    // Handoff lets the next operator compute. Target submission has its own, longer-lived tail.
    private final Map<K, Input> writeTails = new HashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public KeyMoveHandoffs(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
    }

    /** Returns null under backpressure. The caller retries the same input before registering later inputs. */
    public synchronized Input tryRegister(K before, K after) {
        Objects.requireNonNull(before);
        Objects.requireNonNull(after);
        requireActive();
        if (live.size() == capacity) return null;
        boolean move = !before.equals(after);
        CompletableFuture<Void> ready;
        if (move) {
            var earlier = new HashSet<>(pending.getOrDefault(before, Set.of()));
            earlier.addAll(pending.getOrDefault(after, Set.of()));
            ready = CompletableFuture.allOf(earlier.stream().map(input -> input.handed)
                    .toArray(CompletableFuture<?>[]::new));
        } else {
            Input previousMove = orderedTails.get(after);
            ready = previousMove == null ? CompletableFuture.completedFuture(null) : previousMove.handed;
        }
        Input oldWriter = writeTails.get(before);
        Input newWriter = move ? writeTails.get(after) : oldWriter;
        CompletableFuture<Void> mayWrite;
        if (oldWriter == null) {
            mayWrite = newWriter == null ? CompletableFuture.completedFuture(null) : newWriter.settled;
        } else if (newWriter == null || newWriter == oldWriter) {
            mayWrite = oldWriter.settled;
        } else {
            mayWrite = CompletableFuture.allOf(oldWriter.settled, newWriter.settled);
        }
        Input input = new Input(before, after, ready, mayWrite, move);
        writeTails.put(after, input);
        if (move) writeTails.put(before, input);
        live.add(input);
        pending.computeIfAbsent(after, ignored -> new HashSet<>()).add(input);
        if (move) {
            pending.computeIfAbsent(before, ignored -> new HashSet<>()).add(input);
            orderedTails.put(before, input);
        }
        // A later ordinary row must wait behind the first ordinary follower too. Subscribing
        // both to the same completed move would let callback order decide their source order.
        if (move || orderedTails.containsKey(after)) {
            orderedTails.put(after, input);
        }
        return input;
    }

    /** Retires this execution. Notifications from it cannot affect a replacement coordinator. */
    public void abort(Throwable cause) {
        Set<Input> retired;
        synchronized (this) {
            if (!failure.compareAndSet(null, Objects.requireNonNull(cause))) return;
            retired = Set.copyOf(live);
            live.clear();
            pending.clear();
            orderedTails.clear();
            writeTails.clear();
        }
        retired.forEach(input -> input.ready.completeExceptionally(cause));
        retired.forEach(input -> input.handed.completeExceptionally(cause));
        retired.forEach(input -> input.settled.completeExceptionally(cause));
    }

    private void requireActive() {
        Throwable cause = failure.get();
        if (cause != null) throw new IllegalStateException("execution ended", cause);
    }

    private void releaseKey(K key, Input input) {
        var lane = pending.get(key);
        lane.remove(input);
        if (lane.isEmpty()) pending.remove(key);
        orderedTails.remove(key, input);
    }

    public final class Input {
        private final K before;
        private final K after;
        private final CompletableFuture<Void> ready;
        private final CompletableFuture<Void> mayWrite;
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        private final CompletableFuture<Void> handed = new CompletableFuture<>();
        private final Receipt arrival;
        private final Receipt departure;
        private final int sides;
        private int queued;
        private int written;

        private Input(K before, K after, CompletableFuture<Void> ready, CompletableFuture<Void> mayWrite, boolean move) {
            this.before = before;
            this.after = after;
            this.mayWrite = mayWrite;
            // Each blocked input has its own admission gate. A predecessor may already have
            // left live while another thread is still publishing its handoff callbacks.
            this.ready = ready.isDone() ? ready : ready.copy();
            arrival = side(1);
            departure = move ? side(2) : arrival;
            sides = move ? 3 : 1;
        }

        public CompletionStage<Void> ready() { return ready.minimalCompletionStage(); }
        public Receipt departure() { return departure; }
        public Receipt arrival() { return arrival; }

        private Receipt side(int bit) {
            return new Receipt() {
                @Override
                public boolean mayWrite() {
                    requireActive();
                    return mayWrite.isDone() && !mayWrite.isCompletedExceptionally();
                }

                public void queued() { acknowledge(bit, false); }
                public void written() { acknowledge(bit, true); }
            };
        }

        private void acknowledge(int bit, boolean committed) {
            boolean unlock;
            boolean finished;
            synchronized (KeyMoveHandoffs.this) {
                if (!live.contains(this)) return;
                if (!ready.isDone() || ready.isCompletedExceptionally())
                    throw new IllegalStateException("receipt before admission");
                unlock = queued != sides && (queued | bit) == sides;
                queued |= bit;
                if (unlock) {
                    releaseKey(after, this);
                    if (sides == 3) releaseKey(before, this);
                }
                if (committed) written |= bit;
                finished = written == sides;
                if (finished) {
                    live.remove(this);
                    writeTails.remove(after, this);
                    if (sides == 3) writeTails.remove(before, this);
                }
            }
            // Admitting the next input can call back into the coordinator or enter a writer.
            if (finished) settled.complete(null);
            if (unlock) handed.complete(null);
        }
    }
}
