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
}
