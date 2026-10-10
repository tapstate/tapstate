package io.tapstate.control.core;

/** Request-scoped event session exposed to a protocol adapter. */
public interface PipelinePreviewSession extends AutoCloseable {

    /** Blocks until the next event, or returns null after the terminal event has been consumed. */
    PipelinePreviewEvent next() throws InterruptedException;

    /** Whether the matching bounded input sample will be reused. */
    boolean sampleCacheHit();

    /** Cancels connector reads and the finite execution. Safe to call repeatedly. */
    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
