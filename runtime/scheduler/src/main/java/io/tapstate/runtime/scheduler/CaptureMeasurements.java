package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.SnapshotReading;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable projections of one original execution's capture sample, never a reusable authorization. */
public record CaptureMeasurements(SnapshotReading snapshot, SnapshotReading runSnapshot,
        InitialLoad initialLoad, Optional<Map<String, String>> positions) {
    public static final CaptureMeasurements NONE = new CaptureMeasurements(
            SnapshotReading.NONE, SnapshotReading.NONE, null, Optional.empty());

    public CaptureMeasurements {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(runSnapshot, "runSnapshot");
        positions = Objects.requireNonNull(positions, "positions").map(Map::copyOf);
    }
}
