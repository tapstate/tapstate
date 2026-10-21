package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** One latest, server-stamped whole-pool observation from an exact node-session publisher. */
public record ClusterNativeView(WorkloadClaimFence publisherClaim, ClusterExecutionProfile profile,
        long topologyRevision, long viewRevision, String cohortHash, Instant observedAt,
        Instant validUntil, Map<String, ClusterNativeMember> membersByNode) {
    public ClusterNativeView {
        Objects.requireNonNull(publisherClaim, "publisherClaim");
        Objects.requireNonNull(cohortHash, "cohortHash");
        Objects.requireNonNull(observedAt, "observedAt");
        Objects.requireNonNull(validUntil, "validUntil");
        membersByNode = new ClusterNativeObservation(profile, membersByNode).membersByNode();
        ClusterNativeMember publisher = membersByNode.get(publisherClaim.owner().nodeId());
        if (publisherClaim.key().type() != WorkloadClaimType.NODE_SESSION
                || !publisherClaim.key().resourceId().equals(publisherClaim.owner().nodeId())
                || !publisherClaim.key().clusterId().equals(profile.clusterId())
                || publisherClaim.profileGeneration() != profile.generation()
                || publisher == null || !publisher.identity().bootId().equals(publisherClaim.owner().bootId())
                || topologyRevision < 1 || viewRevision < 1 || !cohortHash.matches("[a-f0-9]{64}")
                || !validUntil.isAfter(observedAt)) {
            throw new IllegalArgumentException("native view scope, publisher and server deadline must agree");
        }
    }
}
