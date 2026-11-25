package io.tapstate.spi.capture;

/** Fixed row ordering and an optional saved replay anchor; neither grants source capture authority. */
public record SnapshotPreparation(long snapshotEpoch, SourcePosition replayAnchor) {
    public SnapshotPreparation {
        if (snapshotEpoch < 1 || snapshotEpoch == Long.MAX_VALUE) throw new IllegalArgumentException("invalid snapshot row epoch");
    }
}
