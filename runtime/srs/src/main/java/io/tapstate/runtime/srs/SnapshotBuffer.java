package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * A member-local hand-off from the capture side to the source vertex, keyed by the per-table change ring
 * the source reads. The capture side appends; the source vertex takes what is there and emits it ahead of
 * whatever the ring holds. That ordering is the data-consistency guarantee: a snapshot row (the older
 * value) must reach the sink before any cdc change of the same key, or a stale snapshot would overwrite a
 * newer change.
 *
 * <p>It carries a source's bounded snapshot rows, and on a source running with the shared ring switched
 * off it carries that source's changes too -- there is no ring for them to travel on, so this is the whole
 * of how they reach the sink. That second use is unbounded and lasts as long as the pipeline does, which
 * is why the vertex comes back to it rather than reading it once.
 *
 * <p>It is plain member-local state, never a distributed structure: there is one embedded member per
 * process, and the capture side that writes and the source vertex that reads both run in that one process.
 * Nothing in it needs to survive a member restart, but not because nothing in it is positioned -- a change
 * handed over by a ring-less tail carries its position like any other. It is because a restart re-reads
 * from the durable offset, and that offset only ever moves to what a sink has confirmed: whatever was
 * waiting here when the process died sits above it and is read again. Both sides may touch it
 * concurrently, so it is backed by concurrent maps and queues.
 *
 * <p>A drain is once-consumed: it removes a ring's rows and returns them, so a second drain of the same
 * ring yields only what arrived since. That is what lets the vertex come back to it on every pass without
 * re-emitting anything -- taking whatever is waiting and leaving an empty queue behind.
 */
public final class SnapshotBuffer {

    /**
     * The member user-context key under which the buffer is bound, so a source vertex can resolve it
     * member-side by the ring name it already carries. The assembly layer binds the buffer under this key
     * when it makes the member SRS-capable; a member with no buffer bound emits no snapshot ahead of the tail.
     */
    public static final String USER_CONTEXT_KEY = "tapstate.srs.snapshot-buffer";

    private final ConcurrentMap<String, Queue<Envelope>> byRing = new ConcurrentHashMap<>();

    /** Appends one snapshot row to {@code ringName}'s buffer, preserving append order within the ring. */
    public void append(String ringName, Envelope row) {
        Objects.requireNonNull(ringName, "ringName");
        Objects.requireNonNull(row, "row");
        byRing.computeIfAbsent(ringName, ignored -> new ConcurrentLinkedQueue<>()).add(row);
    }

    /**
     * Removes and returns {@code ringName}'s buffered snapshot rows in append order, or an empty list when
     * the ring was never appended to or has already been drained. Once-consumed: the ring's buffer is
     * cleared, so a later drain returns nothing.
     */
    public List<Envelope> drain(String ringName) {
        Objects.requireNonNull(ringName, "ringName");
        Queue<Envelope> rows = byRing.remove(ringName);
        return rows == null ? List.of() : new ArrayList<>(rows);
    }
}
