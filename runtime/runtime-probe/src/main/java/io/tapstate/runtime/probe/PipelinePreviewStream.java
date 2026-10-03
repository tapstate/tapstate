package io.tapstate.runtime.probe;

/** A request-scoped event stream whose close cancels outstanding connector reads and the Jet job. */
public abstract class PipelinePreviewStream implements AutoCloseable {

    /** Whether this execution will reuse an authorized, matching finite sample. */
    public boolean sampleCacheHit() {
        return false;
    }

    /** Blocks until the next event, or returns null after the terminal event has been consumed. */
    public abstract PipelinePreviewEvent next() throws InterruptedException;

    /** Cancels the execution and releases request-scoped resources. Safe to call repeatedly. */
    public abstract void cancel();

    @Override
    public void close() {
        cancel();
    }
}
