package io.tapstate.spi.capture;

import io.tapstate.core.event.Envelope;
import java.util.Iterator;
import java.util.Optional;

/**
 * A bounded snapshot read: an iterator of events that also holds a source resource, so it must be
 * closed. Every event it yields is a snapshot read (op {@code r}). Closing releases the underlying
 * source; it is idempotent and may be called before the iterator is drained.
 */
public interface CaptureBatch extends Iterator<Envelope>, AutoCloseable {

    /**
     * The position a change tail must resume from to join this snapshot without a gap: sampled when the
     * batch was opened, before its first row was read. Every change made while the snapshot runs
     * therefore falls after it and is re-delivered rather than missed, and the overlap that implies is
     * absorbed by an idempotent write downstream.
     *
     * <p>Sampling it is the source's affair and so is what the token means; reporting it here is what
     * makes the join possible at all. This is the contract's answer to a caller that would otherwise have
     * to make a position up — and a made-up seam is not a smaller version of a real one: the tail starts
     * somewhere the source never was, and whichever changes fall in the difference are gone with nothing
     * thrown and nothing logged.
     *
     * <p>Empty means this batch reports no seam. A source with no change stream to join to is the plain
     * case. Whatever the reason, a caller that needs one and is handed none has to refuse — never fill the
     * gap with a position of its own.
     */
    Optional<SourcePosition> seam();

    /** Releases the underlying source. Idempotent; may be called before the batch is drained. */
    @Override
    void close();
}
