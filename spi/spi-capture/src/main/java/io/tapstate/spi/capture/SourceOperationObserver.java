package io.tapstate.spi.capture;

/** Actual connector operations only. Manual diagnostics never receive an observer. */
@FunctionalInterface
public interface SourceOperationObserver {
    enum Capability { INITIALIZE, METADATA_READ, SNAPSHOT_READ, STREAM_READ, TARGET_READ, TARGET_WRITE }
    void completed(Capability capability, Throwable failure);
    SourceOperationObserver NONE = (capability, failure) -> { };
}
