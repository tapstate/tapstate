package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.SourcePosition;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One source run's live account of itself, readable from outside the thread that runs it: what the run has
 * taken from its source, and whether its stream has died.
 *
 * <p>Both facts are here for the same reason. The stream runs on its own thread and reports through the
 * capture listener; a coordinator polling the run is on another, and has no other way to see either. A
 * failure is otherwise invisible above the run -- the execution job reading the change ring keeps running
 * over a ring that has simply gone quiet, so its status never reflects the tail's death -- and what has
 * arrived is invisible for the same reason, because nothing downstream distinguishes a source with nothing
 * to send from a source that has stopped being read.
 *
 * <p><strong>The rows are counted where the source hands them over, before anything is done with them.</strong>
 * That is the boundary this count is defined at: what the run received, not what it managed to write to a
 * ring or forward to a consumer afterwards. A batch that arrives and then fails to be stored has crossed it,
 * and a batch the source sends a second time crosses it a second time and is counted again -- which is what
 * a count of arrivals means, as against a count of distinct rows.
 */
public final class CaptureHealth {

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /**
     * Rows received, by source table and then by the op's wire symbol. Concurrent on both levels because it
     * is written on whichever thread the connector calls back on -- one per subscription, and a snapshot
     * load on top of it -- and read on the thread that polls the run.
     */
    private final Map<String, Map<String, Long>> received = new ConcurrentHashMap<>();

    /**
     * When this account was opened, which is what its totals count from. Taken here rather than at the first
     * row so that a run which has received nothing still says since when: without it, "nothing has arrived"
     * and "nothing has been measured" are the same reading.
     */
    private final Instant countingSince = Instant.now();

    /** The failure the run's cdc stream died with, or empty while it is healthy or opened no tail. */
    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    /**
     * Records the failure the cdc stream died with. The first is kept: a stream reports at most one, but a
     * compare-and-set keeps the record stable if that ever changes rather than letting a later error
     * overwrite the cause that stopped the tail.
     */
    public void fail(Throwable error) {
        failure.compareAndSet(null, error);
    }

    /** When the counting behind {@link #receivedRows()} began. */
    public Instant countingSince() {
        return countingSince;
    }

    /**
     * What this run has taken from its source, by table and op symbol. A table absent here is one nothing
     * has arrived for, which is not the same as a table that arrived empty -- so a reader can still tell a
     * stream nobody is reading from one that has nothing to say.
     */
    public Map<String, Map<String, Long>> receivedRows() {
        Map<String, Map<String, Long>> snapshot = new LinkedHashMap<>();
        received.forEach((table, byOp) -> snapshot.put(table, Map.copyOf(byOp)));
        return Map.copyOf(snapshot);
    }

    /** Counts one row this run received. Called at every point a source hands one over, and nowhere else. */
    void received(Envelope event) {
        received.computeIfAbsent(event.src(), table -> new ConcurrentHashMap<>())
                .merge(event.op().symbol(), 1L, Long::sum);
    }

    /**
     * Wraps a batch handler as a listener that counts what arrives on this health and records a stream
     * failure on it through {@link #fail}.
     *
     * <p>Every change this run reads passes through here -- a tail that buffers into the ring and one that
     * streams straight to its consumer are both started through a listener built here -- so this is the one
     * place a change is counted, and a fourth way to read one would have to come through it to run at all.
     * Counting happens before the batch is handed on, because arriving is what is being counted: a handler
     * that throws has still been given the rows, and the source will hand them over again.
     *
     * <p>Public so that a caller outside this package can obtain the seam, never so that it can bypass it:
     * counting a row is package-private and has no other way in, so every arrival still goes through here.
     */
    public CaptureListener recording(CaptureListener onBatch) {
        return new CaptureListener() {
            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                events.forEach(CaptureHealth.this::received);
                onBatch.onBatch(events, position);
            }

            @Override
            public void onError(Throwable error) {
                fail(error);
            }
        };
    }
}
