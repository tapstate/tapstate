package io.tapstate.spi.capture;

/** Actual connector operations only. Manual diagnostics never receive an observer. */
@FunctionalInterface
public interface SourceOperationObserver {
    enum Capability { INITIALIZE, METADATA_READ, SNAPSHOT_READ, STREAM_READ, TARGET_READ, TARGET_WRITE }
    void completed(Capability capability, Throwable failure);
    /** A stable operation/table within the original physical access domain. */
    default void completed(String operation,Capability capability,Throwable failure){completed(capability,failure);}
    default SourceOperationObserver fork(String phase){return this;}
    default void close() { }
    SourceOperationObserver NONE = (capability, failure) -> { };
}
