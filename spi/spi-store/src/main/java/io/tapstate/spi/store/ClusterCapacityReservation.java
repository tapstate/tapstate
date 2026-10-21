package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** One exact-intent reservation that becomes the same execution's durable occupancy on submission. */
public record ClusterCapacityReservation(
        String reservationId, String clusterId, String pipelineId, String incarnationId, String intentFingerprint,
        ClusterExecutionProfile profile, WorkloadClaimFence pipelineClaim,
        Map<String, ClusterCapacityDemand> demandByNode, Instant reservedAt, Instant deadline,
        Long executionGeneration, String nativeJobId) {

    public ClusterCapacityReservation {
        Objects.requireNonNull(reservationId, "reservationId");
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(incarnationId, "incarnationId");
        Objects.requireNonNull(intentFingerprint, "intentFingerprint");
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        demandByNode = Map.copyOf(Objects.requireNonNull(demandByNode, "demandByNode"));
        Objects.requireNonNull(reservedAt, "reservedAt");
        Objects.requireNonNull(deadline, "deadline");
        if (reservationId.isBlank() || clusterId.isBlank() || pipelineId.isBlank() || incarnationId.isBlank()
                || intentFingerprint.isBlank() || demandByNode.isEmpty() || !deadline.isAfter(reservedAt)
                || !clusterId.equals(profile.clusterId()) || !clusterId.equals(pipelineClaim.key().clusterId())
                || !pipelineId.equals(pipelineClaim.key().resourceId())
                || pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || (executionGeneration != null && executionGeneration < 1)
                || (nativeJobId != null && (nativeJobId.isBlank() || executionGeneration == null))) {
            throw new IllegalArgumentException("capacity reservation identity or step is invalid");
        }
    }
}
