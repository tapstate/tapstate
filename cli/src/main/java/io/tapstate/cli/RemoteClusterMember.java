package io.tapstate.cli;

/**
 * One cluster member as read back from the server. This mirrors the server's per-member shape
 * independently (rule R6: the CLI carries no shared control type).
 *
 * @param nodeId     the identity that survives a restart -- what a claim names as an owner
 * @param memberUuid the engine's identity for this run of that node's process
 * @param bootId     this boot of that node, which is how a restart is told from a node that stayed
 * @param hzAddress  where members reach each other; never a control address
 * @param controlUrl where a client reaches this member's control face
 * @param state      what the member is to the cluster, in the server's own words
 */
record RemoteClusterMember(
        String nodeId, String memberUuid, String bootId, String hzAddress, String controlUrl, String state,
        Long profileGeneration, String profileHash, String sessionBootId, String sessionLeaseUntil,
        Long sessionLeaseRemainingMillis, Boolean sessionLeased, Boolean joined, String joinedAt, Boolean live,
        String joinedMemberUuid, String joinedMemberAddress) {

    RemoteClusterMember(String nodeId, String memberUuid, String bootId, String hzAddress,
            String controlUrl, String state) {
        this(nodeId, memberUuid, bootId, hzAddress, controlUrl, state,
                null, null, null, null, null, null, null, null, null, null, null);
    }
}
