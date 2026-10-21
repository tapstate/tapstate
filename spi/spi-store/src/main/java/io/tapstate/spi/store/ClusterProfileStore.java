package io.tapstate.spi.store;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Atomic profile admission and the stable-node registry; all lease predicates use store time. */
public interface ClusterProfileStore {
    ClusterNodeReservation reserve(String clusterId, WorkloadOwner owner, URI controlUrl,
            ExecutionProfile proposed, Duration ttl);

    Optional<ClusterExecutionProfile> profile(String clusterId);

    List<ClusterNodeReading> nodes(String clusterId);

    /** Records runtime identity only for the exact still-live node session and profile generation. */
    boolean markJoined(WorkloadClaim expectedSession, String memberUuid, String memberAddress);

    /**
     * Publishes the complete native pool under an exact live node-session and committed revision.
     * The store allocates its hash, revision and time bounds, clamping freshness to the node lease.
     * This records a fact below quorum too; it never returns an admission authority or selects peers.
     */
    default boolean recordNativeView(WorkloadClaim expectedNodeSession, long expectedTopologyRevision,
            ClusterNativeObservation observation, Duration observationTtl) {
        return false;
    }

    /** Absent evidence remains unknown; a present malformed record is a coded storage diagnostic. */
    default Optional<ClusterNativeView> nativeView(String clusterId, String publisherNodeId) {
        return Optional.empty();
    }
}
