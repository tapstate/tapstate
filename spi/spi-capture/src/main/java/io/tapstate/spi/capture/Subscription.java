package io.tapstate.spi.capture;

/**
 * A handle on a running CDC capture. Closing it stops the capture and releases the source; closing
 * is idempotent. While it runs, it can also be told how far its source may release its change log.
 */
public interface Subscription extends AutoCloseable {

    /**
     * Tells the source it may release its change log up to {@code durable}, the position tapstate would
     * itself resume from: nothing before it will be asked of the source again.
     *
     * <p>A source that follows a change log for a reader usually keeps that log until the reader says how
     * far it has got -- a replication slot holds on to every change after the last position confirmed to
     * it -- and a log nobody releases grows until the source runs out of room. This is how the reader
     * says it.
     *
     * <p>What the caller promises:
     * <ul>
     *   <li><b>Only a durable position.</b> {@code durable} has already been written down and read back
     *       with a majority read concern: it is the position tapstate itself would resume from. A source
     *       told anything less could release changes that a restart then asks it for again, and they
     *       would be gone.</li>
     *   <li><b>Forward, but not checked.</b> Positions are passed in the order tapstate came to hold them,
     *       which is forward as a pipeline runs. A position written back by hand is the one exception: it can
     *       be behind one passed before it, and a source keeps whatever it has already released. The same one
     *       may be passed again.</li>
     * </ul>
     *
     * <p>What the call promises back:
     * <ul>
     *   <li><b>It returns at once.</b> The position is applied later, on the thread the source delivers
     *       changes on. A source releases its log over the connection it reads it from, which is rarely
     *       safe to drive from a second thread, and the caller's thread must not wait on a source.</li>
     *   <li><b>Idempotent.</b> Applying the same position again is harmless: it names the same place.</li>
     *   <li><b>Best effort.</b> There is no success signal. Only the source's own readings -- how far it
     *       says its log has been released -- tell whether a position took effect.</li>
     *   <li><b>Never fatal.</b> A position that cannot be applied never closes the stream and is never
     *       thrown to the caller. A release that has not happened yet costs the source some log for a
     *       while longer; ending the stream over it would turn a delay in the source's housekeeping into
     *       an outage.</li>
     *   <li><b>Nothing after close.</b> Once the subscription is closed, this does nothing.</li>
     * </ul>
     *
     * <p>Where a port can say what became of a position, it says so to the listener the stream was started
     * with, through {@link CaptureListener#onAcknowledged} and {@link CaptureListener#onAcknowledgeFailed}.
     * The default does nothing, which is what a port or connector that cannot acknowledge does.
     */
    default void acknowledge(SourcePosition durable) {
    }

    /** Stops the capture and releases the source. Idempotent. */
    @Override
    void close();
}
