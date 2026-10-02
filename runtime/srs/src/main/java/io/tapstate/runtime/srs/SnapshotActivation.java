package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.Subscription;
import java.time.Duration;
import java.util.Map;

/** A reserved snapshot read activated only after the job that drains its buffer has been submitted. */
public interface SnapshotActivation extends Subscription {
    void activateSnapshot();
    default long snapshotRows() { return 0; }
    default Map<String, Long> snapshotRowsByTable() { return Map.of(); }
    default boolean loading() { return false; }
    default boolean awaitLoaded(Duration timeout) throws InterruptedException { return true; }
    default void abandonLoad() { }
}
