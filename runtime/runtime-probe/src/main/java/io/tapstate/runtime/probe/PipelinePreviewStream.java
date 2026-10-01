package io.tapstate.runtime.probe;

/** A request-scoped event stream whose close cancels outstanding connector reads and the Jet job. */
public interface PipelinePreviewStream extends AutoCloseable {

    /** Whether this execution will reuse an authorized, matching finite sample. */
    default boolean sampleCacheHit() {
        return false;
    }

    /** Blocks until the next event, or returns null after the terminal event has been consumed. */
    PipelinePreviewEvent next() throws InterruptedException;

    /** Cancels the execution and releases request-scoped resources. Safe to call repeatedly. */
    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
