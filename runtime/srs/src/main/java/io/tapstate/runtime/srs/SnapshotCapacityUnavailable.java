package io.tapstate.runtime.srs;

/** A transient admission refusal before a snapshot job or source read has been submitted. */
public final class SnapshotCapacityUnavailable extends RuntimeException {
    public SnapshotCapacityUnavailable() {
        super("snapshot data-plane workers are at capacity");
    }
}
