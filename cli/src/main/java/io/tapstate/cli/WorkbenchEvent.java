package io.tapstate.cli;

import java.util.Objects;

/** An immutable value delivered to the workbench reducer on the render thread. */
sealed interface WorkbenchEvent permits WorkbenchEvent.SnapshotExpected, WorkbenchEvent.SnapshotPublished {

    /** Changes the snapshot identity accepted by future worker publications. */
    record SnapshotExpected(WorkbenchSnapshot snapshot) implements WorkbenchEvent {
        public SnapshotExpected {
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }

    /** Publishes the newest completed worker snapshot for reduction on the render thread. */
    record SnapshotPublished(WorkbenchSnapshot snapshot) implements WorkbenchEvent {
        public SnapshotPublished {
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }
}
