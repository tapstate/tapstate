package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.PayloadBytes;
import io.tapstate.core.common.TapstateException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A member-local hand-off from the capture side to the source vertex, keyed by the consumer pipeline and
 * the per-table change ring the source reads. The capture side appends; that pipeline's source vertex takes
 * what is there and emits it ahead of whatever the ring holds. The pipeline coordinate is essential because
 * several pipelines can tail one shared ring while each owns its initial load. Without it, an already-running
 * neighbour can drain a later pipeline's rows into its own target. The ordering within each hand-off is the
 * data-consistency guarantee: a snapshot row (the older value) must reach the sink before any cdc change of
 * the same key, or a stale snapshot would overwrite a newer change.
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
 * <p>Streaming snapshot sessions additionally reserve a fixed number of rows and logical payload bytes
 * across the process and per session. The byte budget uses the product's payload definition plus a fixed
 * envelope allowance; it is a backpressure budget, not an exact JVM heap measurement. A run token keeps an
 * old producer or source from reaching a replacement session, and a terminal marker follows accepted rows.
 *
 * <p>A drain is once-consumed: it removes one pipeline-and-ring hand-off's rows and returns them, so a second
 * drain yields only what arrived since. That is what lets the vertex come back to it on every pass without
 * re-emitting anything. Each drain takes at most the rows counted when it starts; concurrent appends remain
 * for a later pass instead of extending the current drain indefinitely.
 */
public final class SnapshotBuffer {

    private static final int DEFAULT_GLOBAL_ROWS = 4_096;
    private static final int DEFAULT_SESSION_ROWS = 1_024;
    private static final int DEFAULT_GLOBAL_BYTES = 64 * 1024 * 1024;
    private static final int DEFAULT_SESSION_BYTES = 16 * 1024 * 1024;

    public enum SessionState { ACTIVE, DONE, FAILED, CANCELLED }

    public record SessionDrain(List<Envelope> rows, SessionState state, Throwable failure) {
        public SessionDrain {
            rows = List.copyOf(rows);
        }
    }

    private sealed interface SessionItem permits Row, Terminal {
    }

    private record Row(Envelope envelope, int bytes) implements SessionItem {
    }

    private record Terminal(SessionState state, Throwable failure) implements SessionItem {
    }

    /**
     * The member user-context key under which the buffer is bound, so a source vertex can resolve it
     * member-side by the ring name it already carries. The assembly layer binds the buffer under this key
     * when it makes the member SRS-capable; a member with no buffer bound emits no snapshot ahead of the tail.
     */
    public static final String USER_CONTEXT_KEY = "tapstate.srs.snapshot-buffer";

    private final ConcurrentMap<BufferKey, Queue<Envelope>> byConsumerRing = new ConcurrentHashMap<>();
    private final ConcurrentMap<BufferKey, Session> sessions = new ConcurrentHashMap<>();
    private final Semaphore globalRows;
    private final Semaphore globalBytes;
    private final int sessionRowBudget;
    private final int sessionByteBudget;
    private final int globalByteBudget;

    public SnapshotBuffer() {
        this(DEFAULT_GLOBAL_ROWS, DEFAULT_SESSION_ROWS, DEFAULT_GLOBAL_BYTES, DEFAULT_SESSION_BYTES);
    }

    public SnapshotBuffer(int globalRows, int sessionRows, int globalBytes, int sessionBytes) {
        if (sessionRows < 1 || globalRows < sessionRows || sessionBytes < 1 || globalBytes < sessionBytes) {
            throw new IllegalArgumentException("snapshot session budgets must be positive and fit globally");
        }
        this.globalRows = new Semaphore(globalRows);
        this.globalBytes = new Semaphore(globalBytes);
        this.sessionRowBudget = sessionRows;
        this.sessionByteBudget = sessionBytes;
        this.globalByteBudget = globalBytes;
    }

    /** Starts one bounded snapshot stream; a previous run of the same ring is cancelled before replacement. */
    public void beginSnapshot(String pipelineId, String ringName, String token) {
        Objects.requireNonNull(token, "token");
        BufferKey key = new BufferKey(pipelineId, ringName);
        Session next = new Session(key, token);
        Session previous = sessions.put(key, next);
        if (previous != null) {
            previous.release();
        }
    }

    /** Waits for bounded row and logical-byte capacity, then appends under the exact run token. */
    public void appendSnapshot(String pipelineId, String ringName, String token, Envelope row) {
        Objects.requireNonNull(row, "row");
        Session session = requireSession(new BufferKey(pipelineId, ringName), token);
        long logicalBytes = 128L + row.src().getBytes(StandardCharsets.UTF_8).length + PayloadBytes.of(row);
        if (logicalBytes > sessionByteBudget || logicalBytes > globalByteBudget) {
            throw new TapstateException(CaptureError.SNAPSHOT_ROW_TOO_LARGE,
                    Map.of("pipeline", pipelineId, "bytes", logicalBytes, "limitBytes", sessionByteBudget), null);
        }
        session.append(row, Math.toIntExact(logicalBytes));
    }

    public void completeSnapshot(String pipelineId, String ringName, String token) {
        requireSession(new BufferKey(pipelineId, ringName), token).finish(SessionState.DONE, null);
    }

    public void failSnapshot(String pipelineId, String ringName, String token, Throwable failure) {
        requireSession(new BufferKey(pipelineId, ringName), token)
                .finish(SessionState.FAILED, Objects.requireNonNull(failure, "failure"));
    }

    public void cancelSnapshot(String pipelineId, String ringName, String token) {
        requireSession(new BufferKey(pipelineId, ringName), token).finish(SessionState.CANCELLED, null);
    }

    /** Reads at most {@code maxRows}; DONE is visible only after every earlier row has been drained. */
    public SessionDrain drainSnapshot(String pipelineId, String ringName, String token, int maxRows) {
        if (maxRows < 1) {
            throw new IllegalArgumentException("a snapshot drain must request at least one row");
        }
        BufferKey key = new BufferKey(pipelineId, ringName);
        Session session = sessions.get(key);
        if (session == null || !session.token.equals(token)) {
            return new SessionDrain(List.of(), SessionState.CANCELLED, null);
        }
        return session.drain(maxRows);
    }

    private Session requireSession(BufferKey key, String token) {
        Session session = sessions.get(key);
        if (session == null || !session.token.equals(token)) {
            throw new CancellationException("snapshot session was replaced or released");
        }
        return session;
    }

    private final class Session {
        private final BufferKey key;
        private final String token;
        private final Semaphore rows = new Semaphore(sessionRowBudget);
        private final Semaphore bytes = new Semaphore(sessionByteBudget);
        private final ArrayBlockingQueue<SessionItem> queued = new ArrayBlockingQueue<>(sessionRowBudget + 1);
        private volatile SessionState producerState = SessionState.ACTIVE;
        private SessionState drainedState = SessionState.ACTIVE;
        private Throwable drainedFailure;

        private Session(BufferKey key, String token) {
            this.key = key;
            this.token = token;
        }

        private void append(Envelope row, int weight) {
            boolean localRow = false;
            boolean localBytes = false;
            boolean sharedRow = false;
            boolean sharedBytes = false;
            try {
                acquire(rows, 1); localRow = true;
                acquire(bytes, weight); localBytes = true;
                acquire(globalRows, 1); sharedRow = true;
                acquire(globalBytes, weight); sharedBytes = true;
                synchronized (this) {
                    active();
                    if (!queued.offer(new Row(row, weight))) {
                        throw new IllegalStateException("reserved snapshot row slot was not available");
                    }
                }
                localRow = localBytes = sharedRow = sharedBytes = false;
            } finally {
                if (sharedBytes) globalBytes.release(weight);
                if (sharedRow) globalRows.release();
                if (localBytes) bytes.release(weight);
                if (localRow) rows.release();
            }
        }

        private void acquire(Semaphore capacity, int permits) {
            try {
                while (true) {
                    active();
                    if (capacity.tryAcquire(permits, 50, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                }
            } catch (InterruptedException cancelled) {
                Thread.currentThread().interrupt();
                throw new CancellationException("snapshot producer was interrupted");
            }
        }

        private void active() {
            if (producerState != SessionState.ACTIVE || sessions.get(key) != this) {
                throw new CancellationException("snapshot session is no longer active");
            }
        }

        private synchronized void finish(SessionState terminal, Throwable failure) {
            if (producerState != SessionState.ACTIVE) {
                return;
            }
            producerState = terminal;
            if (terminal == SessionState.CANCELLED) {
                SessionItem item;
                while ((item = queued.poll()) != null) {
                    if (item instanceof Row row) {
                        release(row);
                    }
                }
            }
            if (!queued.offer(new Terminal(terminal, failure))) {
                throw new IllegalStateException("snapshot terminal slot was not available");
            }
        }

        private synchronized void release() {
            producerState = SessionState.CANCELLED;
            drainedState = SessionState.CANCELLED;
            drainedFailure = null;
            SessionItem item;
            while ((item = queued.poll()) != null) {
                if (item instanceof Row row) {
                    release(row);
                }
            }
        }

        private synchronized SessionDrain drain(int maxRows) {
            List<Envelope> out = new ArrayList<>();
            while (out.size() < maxRows) {
                SessionItem item = queued.poll();
                if (item == null) {
                    break;
                }
                if (item instanceof Row row) {
                    out.add(row.envelope());
                    release(row);
                } else if (item instanceof Terminal terminal) {
                    drainedState = terminal.state();
                    drainedFailure = terminal.failure();
                    break;
                }
            }
            return new SessionDrain(out, drainedState, drainedFailure);
        }

        private void release(Row row) {
            rows.release();
            bytes.release(row.bytes());
            globalRows.release();
            globalBytes.release(row.bytes());
        }
    }

    /** Appends one row to a pipeline's buffer for {@code ringName}, preserving append order. */
    public void append(String pipelineId, String ringName, Envelope row) {
        Objects.requireNonNull(row, "row");
        byConsumerRing.computeIfAbsent(new BufferKey(pipelineId, ringName), ignored -> new LinkedBlockingQueue<>())
                .add(row);
    }

    /**
     * Removes and returns the rows currently buffered for this pipeline and {@code ringName} in append order,
     * or an empty list when none are waiting. Concurrent appends remain for a later drain; every row is
     * consumed once by the pipeline it belongs to.
     */
    public List<Envelope> drain(String pipelineId, String ringName) {
        // Keep the queue attached: an append may already hold it but not yet have inserted its row.
        Queue<Envelope> rows = byConsumerRing.get(new BufferKey(pipelineId, ringName));
        if (rows == null) return List.of();
        // LinkedBlockingQueue reads its count without walking a tail that capture can keep extending.
        // Take the whole initial batch so seeded snapshot rows still precede any change off the ring.
        int remaining = rows.size();
        List<Envelope> drained = new ArrayList<>();
        Envelope row;
        while (remaining-- > 0 && (row = rows.poll()) != null) {
            drained.add(row);
        }
        return drained;
    }

    /**
     * Releases every hand-off owned by a pipeline once its capture has stopped and its source vertex has
     * been cancelled. Live drains deliberately leave their queues attached to protect an append that already
     * holds one; lifecycle release is the point where both the queues and their coordinate strings can go.
     * A neighbouring consumer of the same ring is unaffected.
     */
    public void release(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        sessions.entrySet().removeIf(entry -> {
            if (!entry.getKey().pipelineId().equals(pipelineId)) {
                return false;
            }
            entry.getValue().release();
            return true;
        });
        byConsumerRing.keySet().removeIf(key -> key.pipelineId().equals(pipelineId));
    }

    /** The two coordinates that make a buffered row private to one consumer of a shared ring. */
    record BufferKey(String pipelineId, String ringName) {
        BufferKey {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(ringName, "ringName");
        }
    }
}
