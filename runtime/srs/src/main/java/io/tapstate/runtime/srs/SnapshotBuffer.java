package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A member-local hand-off from the capture side to the source vertex, keyed by the consumer pipeline and
 * the per-table change ring the source reads. The capture side appends; that pipeline's source vertex takes
 * what is there and emits it ahead of whatever the ring holds. The pipeline coordinate is essential because
 * several pipelines can tail one shared ring while each owns its initial load. Without it, an already-running
 * neighbour can drain a later pipeline's rows into its own target. The ordering within each hand-off is the
 * data-consistency guarantee: a snapshot row (the older value) must reach the sink before any cdc change of
 * the same key, or a stale snapshot would overwrite a newer change.
 *
 * <p>It carries a source's snapshot rows, and on a source running with the shared ring switched off it
 * carries that source's changes too -- there is no ring for them to travel on, so this is the whole of how
 * they reach the sink. That second use lasts as long as the pipeline does, which is why the vertex comes
 * back to it rather than reading it once.
 *
 * <p><b>A declared load is held to a fixed number of rows, and its reader waits for room.</b> An initial
 * load is read while the job that takes it is running, so a table of any size reaches the sink through a
 * hand-off of the same small size: when {@link #capacity} of its rows are waiting, the next append parks
 * the thread reading the source until the vertex has taken some, and the pause travels on to the
 * connector's own read. Nothing else here decides how much of a source is on the heap at once. The
 * alternative this replaced read every row of every table into this hand-off before the job existed, so
 * the heap a load needed was the size of the load, and a load larger than the heap took the process down
 * part way through with nothing to show for it.
 *
 * <p>A load is declared before the job that takes it is submitted, ended once its last row has been
 * appended, and read by the vertex through {@link #snapshotState}: a declared load that has not been handed
 * over in full holds the vertex off the shared ring, because a change read from the ring ahead of a
 * snapshot row of the same key would be overwritten by the older value. Only snapshot rows appended while
 * the load is declared and not ended wait for room; a change appended after it ended does not, and neither
 * does anything appended under a coordinate nobody declared, which is the hand-off exactly as it was before
 * loads were declared.
 *
 * <p>It is plain member-local state, never a distributed structure: there is one embedded member per
 * process, and the capture side that writes and the source vertex that reads both run in that one process.
 * Nothing in it needs to survive a member restart, but not because nothing in it is positioned -- a change
 * handed over by a ring-less tail carries its position like any other. It is because a restart re-reads
 * from the durable offset, and that offset only ever moves to what a sink has confirmed: whatever was
 * waiting here when the process died sits above it and is read again. Both sides may touch it
 * concurrently, so it is backed by concurrent maps and queues.
 *
 * <p>A drain is once-consumed: it removes one pipeline-and-ring hand-off's rows and returns them, so a second
 * drain yields only what arrived since. That is what lets the vertex come back to it on every pass without
 * re-emitting anything. Each drain takes at most the rows counted when it starts; concurrent appends remain
 * for a later pass instead of extending the current drain indefinitely.
 */
public final class SnapshotBuffer {

    /**
     * The member user-context key under which the buffer is bound, so a source vertex can resolve it
     * member-side by the ring name it already carries. The assembly layer binds the buffer under this key
     * when it makes the member SRS-capable; a member with no buffer bound emits no snapshot ahead of the tail.
     */
    public static final String USER_CONTEXT_KEY = "tapstate.srs.snapshot-buffer";

    /**
     * How many rows of one declared load may wait in one pipeline's hand-off for one ring before the reader
     * appending them waits for room. A few batches of a source read: enough that the vertex never runs dry
     * while the reader fetches the next batch, and small enough that a server holds the same few megabytes
     * per table whatever the table's size.
     */
    public static final int DEFAULT_CAPACITY = 8_192;

    private final int capacity;

    private final ConcurrentMap<BufferKey, Queue<Envelope>> byConsumerRing = new ConcurrentHashMap<>();

    /** The loads declared on this member, by the same coordinate as the rows they hand over. */
    private final ConcurrentMap<BufferKey, Handoff> handoffs = new ConcurrentHashMap<>();

    public SnapshotBuffer() {
        this(DEFAULT_CAPACITY);
    }

    /** A buffer whose declared loads wait for room once {@code capacity} of their rows are waiting. */
    public SnapshotBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("a hand-off holds at least one row, got " + capacity);
        }
        this.capacity = capacity;
    }

    /** How many rows of one declared load wait at most in one hand-off. */
    public int capacity() {
        return capacity;
    }

    /**
     * Says that a load of {@code ringName}'s table is coming for this pipeline, before the job that takes it
     * is submitted: its rows follow, then {@link #endSnapshot}. The source vertex reads the declaration when
     * it starts, which is why it has to precede the job -- a vertex that found none would read the ring at
     * once and could emit a change ahead of the snapshot row it supersedes.
     *
     * <p>A declaration replaces whatever an earlier one left under the same coordinate: it opens this run's
     * load, and nothing a previous run of the pipeline handed over belongs to it.
     */
    public void declareSnapshot(String pipelineId, String ringName) {
        handoffs.put(new BufferKey(pipelineId, ringName), new Handoff());
    }

    /**
     * Says that every row of the declared load has been appended. Ending a load nobody declared -- or one a
     * stop has already released -- does nothing.
     */
    public void endSnapshot(String pipelineId, String ringName) {
        Handoff handoff = handoffs.get(new BufferKey(pipelineId, ringName));
        if (handoff != null) {
            handoff.end();
        }
    }

    /**
     * Where this pipeline's load of {@code ringName}'s table stands, as the source vertex needs to know it:
     * whether one was declared at all, whether any of its rows has already been taken, and whether every
     * one of them has.
     */
    public SnapshotState snapshotState(String pipelineId, String ringName) {
        Handoff handoff = handoffs.get(new BufferKey(pipelineId, ringName));
        return handoff == null ? SnapshotState.UNDECLARED : handoff.state();
    }

    /**
     * Appends one row to a pipeline's buffer for {@code ringName}, preserving append order.
     *
     * <p>A row of a declared load that has not ended waits while {@link #capacity} of its rows are already
     * waiting here. The wait ends when the vertex takes some, and gives up with a
     * {@link CancellationException} when the pipeline's hand-offs are released or the waiting thread is
     * interrupted -- both mean the load is being abandoned, and the row is not appended.
     */
    public void append(String pipelineId, String ringName, Envelope row) {
        Objects.requireNonNull(row, "row");
        BufferKey key = new BufferKey(pipelineId, ringName);
        Handoff handoff = handoffs.get(key);
        if (handoff == null) {
            queueOf(key).add(row);
            return;
        }
        handoff.append(() -> queueOf(key).add(row), capacity);
    }

    /**
     * Removes and returns the rows currently buffered for this pipeline and {@code ringName} in append order,
     * or an empty list when none are waiting. Concurrent appends remain for a later drain; every row is
     * consumed once by the pipeline it belongs to.
     */
    public List<Envelope> drain(String pipelineId, String ringName) {
        BufferKey key = new BufferKey(pipelineId, ringName);
        // Keep the queue attached: an append may already hold it but not yet have inserted its row.
        Queue<Envelope> rows = byConsumerRing.get(key);
        if (rows == null) return List.of();
        // LinkedBlockingQueue reads its count without walking a tail that capture can keep extending.
        // Take the whole initial batch so seeded snapshot rows still precede any change off the ring.
        int remaining = rows.size();
        List<Envelope> drained = new ArrayList<>();
        Envelope row;
        while (remaining-- > 0 && (row = rows.poll()) != null) {
            drained.add(row);
        }
        Handoff handoff = handoffs.get(key);
        if (handoff != null && !drained.isEmpty()) {
            handoff.taken(drained.size());
        }
        return drained;
    }

    /**
     * Releases every hand-off owned by a pipeline once its capture has stopped and its source vertex has
     * been cancelled. Live drains deliberately leave their queues attached to protect an append that already
     * holds one; lifecycle release is the point where both the queues and their coordinate strings can go.
     * A load still waiting for room is woken and abandoned. A neighbouring consumer of the same ring is
     * unaffected.
     */
    public void release(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        byConsumerRing.keySet().removeIf(key -> key.pipelineId().equals(pipelineId));
        handoffs.entrySet().removeIf(entry -> {
            if (!entry.getKey().pipelineId().equals(pipelineId)) {
                return false;
            }
            entry.getValue().release();
            return true;
        });
    }

    private Queue<Envelope> queueOf(BufferKey key) {
        return byConsumerRing.computeIfAbsent(key, ignored -> new LinkedBlockingQueue<>());
    }

    /**
     * Where one declared load stands.
     *
     * @param declared   whether a load was declared under this coordinate; a coordinate nobody declared is
     *                   a hand-off with no load to wait for
     * @param begun      whether any row of the load has already been taken. A vertex that starts on a load
     *                   somebody else began taking cannot vouch for rows it never saw
     * @param handedOver whether every row of the load has been appended and taken, so nothing of it is left
     *                   to arrive ahead of the ring
     */
    public record SnapshotState(boolean declared, boolean begun, boolean handedOver) {

        /** A coordinate with no declared load: nothing to wait for and nothing to vouch for. */
        public static final SnapshotState UNDECLARED = new SnapshotState(false, false, true);
    }

    /**
     * One declared load: how many of its rows are waiting, and whether it has ended, been started on or been
     * released. The rows themselves wait in the coordinate's queue, ahead of anything appended after the
     * load ended, so the load's rows are always the first ones a drain takes.
     */
    private static final class Handoff {

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition room = lock.newCondition();
        private int waiting;
        private boolean ended;
        private boolean begun;
        private boolean released;

        void append(Runnable enqueue, int capacity) {
            lock.lock();
            try {
                while (!ended && !released && waiting >= capacity) {
                    room.await();
                }
                if (released) {
                    throw new CancellationException("the pipeline's hand-off was released while its load ran");
                }
                enqueue.run();
                if (!ended) {
                    waiting++;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CancellationException("the load was interrupted while it waited for room");
            } finally {
                lock.unlock();
            }
        }

        /**
         * Counts {@code rows} off the head of the queue. The load's rows are at the head, so however many of
         * a drain's rows there are, the first ones are the load's -- and anything past them was appended
         * after it ended, which never counted. A drain that took none of the load's rows leaves it as it
         * was: only a row of the load having been taken is what a later vertex cannot vouch for.
         */
        void taken(int rows) {
            lock.lock();
            try {
                if (waiting > 0) {
                    begun = true;
                    waiting -= Math.min(rows, waiting);
                    room.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        void end() {
            lock.lock();
            try {
                ended = true;
                room.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void release() {
            lock.lock();
            try {
                released = true;
                room.signalAll();
            } finally {
                lock.unlock();
            }
        }

        SnapshotState state() {
            lock.lock();
            try {
                return new SnapshotState(true, begun, ended && waiting == 0);
            } finally {
                lock.unlock();
            }
        }
    }

    /** The two coordinates that make a buffered row private to one consumer of a shared ring. */
    record BufferKey(String pipelineId, String ringName) {
        BufferKey {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(ringName, "ringName");
        }
    }
}
