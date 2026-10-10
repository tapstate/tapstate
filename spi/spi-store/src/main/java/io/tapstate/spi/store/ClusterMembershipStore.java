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

    /**
     * Begins the already-admitted profile's bootstrap set under an exact live node session. A newer
     * profile is the only authority to replace an older profile's set; topology revision stays
     * monotonic. The profile/session documents participate in the same real write competition.
     */
    default Optional<ClusterMembership> initializeProfile(
            WorkloadClaim nodeSession, long expectedRevision, Set<String> activeNodeIds) {
        throw new UnsupportedOperationException("profile-aware membership bootstrap is not configured");
    }

    /** Adds members within the same exact profile; a departure cannot shrink its quorum denominator. */
    default Optional<ClusterMembership> compareAndSetProfile(
            WorkloadClaim nodeSession, long expectedRevision, Set<String> activeNodeIds) {
        throw new UnsupportedOperationException("profile-aware membership writes are not configured");
    }
}
