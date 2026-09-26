package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.Subscription;

/** A reserved snapshot read activated only after the job that drains its buffer has been submitted. */
public interface SnapshotActivation extends Subscription {
    void activateSnapshot();
}
