package io.tapstate.spi.store;

import java.util.Optional;
import java.util.Set;

/** Atomic durable registry of the last ACTIVE membership agreed by the prior committed majority. */
public interface ClusterMembershipStore {

    Optional<ClusterMembership> read(String clusterId);

    /** Creates revision one only when absent and returns the stored winner. */
    ClusterMembership createIfAbsent(String clusterId, Set<String> activeNodeIds);

    /** Replaces exactly {@code expectedRevision} with the next revision. */
    Optional<ClusterMembership> compareAndSet(
            String clusterId, long expectedRevision, Set<String> activeNodeIds);
}
