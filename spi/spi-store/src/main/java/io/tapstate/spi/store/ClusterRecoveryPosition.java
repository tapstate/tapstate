package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Objects;

/** Original resume provenance; snapshot state remains owned by the capture/SRS truth layer. */
public record ClusterRecoveryPosition(
        String sourceId, String connectorId, String captureId, Kind kind, ChainPosition position,
        String provenance, String durableStateReference) {
    public enum Kind {
        DURABLE_POSITION,
        SNAPSHOT_REQUIRED
    }

    public ClusterRecoveryPosition {
        sourceId = ClusterRecoveryKey.required(sourceId, "sourceId");
        connectorId = ClusterRecoveryKey.required(connectorId, "connectorId");
        captureId = ClusterRecoveryKey.required(captureId, "captureId");
        kind = Objects.requireNonNull(kind, "kind");
        provenance = ClusterRecoveryKey.required(provenance, "provenance");
        if ((kind == Kind.DURABLE_POSITION && position == null)
                || (kind == Kind.DURABLE_POSITION && position.order() == null && position.token() == null)
                || (kind == Kind.SNAPSHOT_REQUIRED && position != null)) {
            throw new IllegalArgumentException("resume position must match the explicit resume kind");
        }
        if (durableStateReference != null) {
            durableStateReference = ClusterRecoveryKey.required(durableStateReference, "durableStateReference");
        }
    }
}
