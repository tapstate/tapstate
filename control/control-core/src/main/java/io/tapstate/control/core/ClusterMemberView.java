package io.tapstate.control.core;

import java.util.Objects;
import java.time.Instant;

/**
 * One live member or registered node. The session boot identifies the durable proof separately from
 * the observed runtime boot; a conflict never borrows the new boot's lease for the old member.
 * The signed remaining lease is measured by the store. Join time proves a join, not a heartbeat.
 */
public record ClusterMemberView(
        String nodeId,
        String memberUuid,
        String bootId,
        String hzAddress,
        String controlUrl,
        ClusterMemberState state,
        Long profileGeneration,
        String profileHash,
        String sessionBootId,
        Instant sessionLeaseUntil,
        Long sessionLeaseRemainingMillis,
        Boolean sessionLeased,
        Boolean joined,
        Instant joinedAt,
        Boolean live,
        String joinedMemberUuid,
        String joinedMemberAddress) {

    public ClusterMemberView {
        Objects.requireNonNull(state, "state");
    }

    /** A member for which no durable node-session or profile proof was read. */
    public ClusterMemberView(String nodeId, String memberUuid, String bootId, String hzAddress,
            String controlUrl, ClusterMemberState state) {
        this(nodeId, memberUuid, bootId, hzAddress, controlUrl, state,
                null, null, null, null, null, null, null, null, null, null, null);
    }
}
