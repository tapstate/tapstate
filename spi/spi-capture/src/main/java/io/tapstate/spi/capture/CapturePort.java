package io.tapstate.spi.capture;

import io.tapstate.core.common.TapstateException;

import java.util.Optional;

/**
 * The read side of a connector: bounded snapshot reads, an unbounded CDC stream, a connection test
 * and schema discovery. A pure interface over the standard event envelope; it depends on the core
 * ring only (rule R2) and names no connector-specific type.
 *
 * <p>Read-side boundary: {@link #snapshot} yields snapshot reads (op {@code r}); {@link #cdc} yields
 * row and schema mutations (ops {@code i} / {@code u} / {@code d} / {@code ddl}).
 *
 * <p>Position is part of these signatures, on both sides. A cdc stream is told where to begin as a
 * {@link CaptureStart}, so no port decides that for a caller who did not say; a snapshot batch reports
 * the {@link CaptureBatch#seam() seam} its change tail has to resume from, so no caller invents a
 * position the source never reported. Between them they carry the obligation that used to sit outside
 * the contract entirely: stitching a snapshot to its change tail without a gap is the source's own
 * affair, and the port is where the source says what makes it possible.
 *
 * <p>How the two reads compose is driven by the pipeline read mode, read as a {@link CapturePlan}:
 * {@code snapshot_and_cdc} runs {@link #snapshot} then {@link #cdc} resuming at the batch's seam,
 * {@code cdc_only} runs {@link #cdc} alone from wherever its caller starts it, {@code snapshot_only}
 * runs a bounded read alone, using {@link SnapshotOnlyCapture} where offered to avoid sampling an unused
 * seam. The two {@link CapturePhase}s classify each yielded event by its op; the caller persists the
 * positions it is handed.
 */
public interface CapturePort {

    /**
     * Reads the configured streams once, as a bounded batch of snapshot-read events. The returned
     * batch holds a source resource and must be closed.
     */
    CaptureBatch snapshot(CaptureConfig config);

    /**
     * Starts an unbounded CDC stream at {@code start}, delivering each change event to {@code listener}.
     * The returned subscription stops the stream when closed.
     */
    Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener);

    /**
     * Tests the connection and returns what it found: the schema the source exposes and a small
     * sample of events.
     */
    ConnectionReport testConnection(CaptureConfig config);

    /** Discovers the streams and fields the source exposes. */
    DiscoveredSchema discoverSchema(CaptureConfig config);

    /**
     * Lets go of what the connector set up on the source to read changes through {@code config}'s notes -- a
     * replication slot, say. Called once nothing reads through those notes any more and the state they belong
     * to is being cleared: a resource left behind there is one nobody confirms again, and a source keeps its
     * whole change log for it.
     *
     * <p>A source that refuses, or cannot be reached, keeps what it had. That is answered rather than thrown,
     * as a coded refusal naming what is left there for somebody to remove by hand, because it must not stop
     * the state being cleared. The notes are left as they are: they are the only record of what was set up,
     * so dropping them is the caller's, after this returns. A port or connector with nothing to let go of
     * answers empty, which is the default.
     */
    default Optional<TapstateException> release(CaptureConfig config) {
        return Optional.empty();
    }
}
