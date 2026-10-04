package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.PayloadBytes;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
 *
 * <p>The payload those rows carried is measured at that same point and off those same events, so the two
 * readings cannot come to cover different arrivals. How many rows arrived and how much data they were are
 * different questions: a table of wide rows and a table of narrow ones answer the first identically.
 *
 * <p>It also says whether the source is still being told how far it may release its change log, and that is
 * a reading, never a failure: a release that has not happened yet costs the source some log for a while
 * longer and nothing else.
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
     * Payload bytes received, by source table. One level of concurrency rather than two, because a byte
     * count is broken out by the table alone -- the operation a row came from says nothing about how much
     * of it there was, and a dimension nothing reads is a dimension that goes wrong unnoticed.
     */
    private final Map<String, Long> bytesReceived = new ConcurrentHashMap<>();

    /**
     * When this account was opened, which is what its totals count from. Taken here rather than at the first
     * row so that a run which has received nothing still says since when: without it, "nothing has arrived"
     * and "nothing has been measured" are the same reading.
     */
    private final Instant countingSince = Instant.now();

    /** Acknowledgements that have failed since the last one that went through. */
    private final AtomicLong acknowledgeFailures = new AtomicLong();

    /** The code the last failed acknowledgement carried; null before any failed, or when it carried none. */
    private final AtomicReference<String> lastAcknowledgeFailureCode = new AtomicReference<>();

    /** When a position last went through to the source; null before the first. */
    private final AtomicReference<Instant> lastAcknowledgedAt = new AtomicReference<>();

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

    /**
     * How many bytes of payload this run has taken from its source, by table. A table absent here is one
     * nothing has arrived for, as with the counts beside it; a table present at nought is one whose rows
     * carried no payload, which is what a stream of schema changes alone is.
     */
    public Map<String, Long> receivedBytes() {
        return Map.copyOf(bytesReceived);
    }

    /**
     * Counts one row this run received, and what its payload weighed. Called at every point a source hands
     * one over, and nowhere else.
     *
     * <p>Both readings are taken off the one event in the one call, so neither can drift onto a different
     * set of arrivals than the other. What is added for weight is the product's own definition of a row's
     * payload and not a figure read off a driver or a serializer, so it does not move when either does.
     */
    /**
     * How many acknowledgements in a row have failed to reach the source: each failure counts one more, and
     * the next one that goes through puts it back to nought. A count that keeps climbing says the source is
     * not being released at all.
     *
     * <p><strong>Never a failure of the run.</strong> {@link #failure()} is shared by every pipeline reading
     * the capture and fails all of them, which a late release does not warrant: what it costs is the source
     * keeping some log a while longer, so it is counted here and nothing is failed.
     */
    public long consecutiveAcknowledgeFailures() {
        return acknowledgeFailures.get();
    }

    /**
     * The error code the last failed acknowledgement carried, or empty before any has failed or when the last
     * one carried none. Kept after a later success: it says what the last failure was, and the count beside
     * it says whether failures are still happening.
     */
    public Optional<String> lastAcknowledgeFailureCode() {
        return Optional.ofNullable(lastAcknowledgeFailureCode.get());
    }

    /**
     * When a position was last handed to the source and taken without complaint, or empty before the first.
     * Not proof that the source released anything -- a source gives no such sign -- but a time that stops
     * moving says positions have stopped reaching it.
     */
    public Optional<Instant> lastAcknowledgedAt() {
        return Optional.ofNullable(lastAcknowledgedAt.get());
    }

    /** Records a position that went through to the source: the run of failures is over, and when it ended. */
    private void acknowledged() {
        acknowledgeFailures.set(0);
        lastAcknowledgedAt.set(Instant.now());
    }

    /**
     * Records a position that did not reach the source, and the code it failed with when it carries one --
     * whether the source refused it, or the position could not even be read to be handed over.
     */
    void acknowledgeFailed(Throwable acknowledgeFailure) {
        acknowledgeFailures.incrementAndGet();
        lastAcknowledgeFailureCode.set(
                acknowledgeFailure instanceof TapstateException coded ? coded.code().code() : null);
    }

    void received(Envelope event) {
        received.computeIfAbsent(event.src(), table -> new ConcurrentHashMap<>())
                .merge(event.op().symbol(), 1L, Long::sum);
        bytesReceived.merge(event.src(), PayloadBytes.of(event), Long::sum);
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
     *
     * <p>What became of an acknowledged position is read off here too, and handed on. A failed one is never
     * recorded through {@link #fail}: that would fail every pipeline reading the capture over a release that is
     * only late.
     */
    public CaptureListener recording(CaptureListener onBatch) {
        CaptureListener recorded = new CaptureListener() {
            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                events.forEach(CaptureHealth.this::received);
                onBatch.onBatch(events, position);
            }

            @Override
            public void onError(Throwable error) {
                fail(error);
            }

            @Override
            public void onAcknowledged(SourcePosition position) {
                acknowledged();
                onBatch.onAcknowledged(position);
            }

            @Override
            public void onAcknowledgeFailed(Throwable acknowledgeFailure) {
                acknowledgeFailed(acknowledgeFailure);
                onBatch.onAcknowledgeFailed(acknowledgeFailure);
            }
        };
        if (!(onBatch instanceof CaptureStartedListener started)) {
            return recorded;
        }
        return new CaptureStartedListener() {
            @Override
            public void onStart(SourcePosition position) {
                started.onStart(position);
            }

            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                recorded.onBatch(events, position);
            }

            @Override
            public void onError(Throwable error) {
                recorded.onError(error);
            }

            @Override
            public void onAcknowledged(SourcePosition position) {
                recorded.onAcknowledged(position);
            }

            @Override
            public void onAcknowledgeFailed(Throwable acknowledgeFailure) {
                recorded.onAcknowledgeFailed(acknowledgeFailure);
            }
        };
    }
}
