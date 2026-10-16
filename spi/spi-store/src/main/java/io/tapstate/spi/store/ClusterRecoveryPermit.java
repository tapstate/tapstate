package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * One durable recovery slot and per-member budget reservation, with a store-clock deadline.
 * The transferred generation binds a real allocator result; native submission and startup remain separate receipts.
 */
public record ClusterRecoveryPermit(
        String reservationId, WorkloadClaimFence recoveryClaim, Instant reservedAt, Instant deadline,
        Map<String, ClusterCapacityDemand> demandByNode, long transferredExecutionGeneration) {
    public ClusterRecoveryPermit {
        reservationId = ClusterRecoveryKey.required(reservationId, "reservationId");
        recoveryClaim = Objects.requireNonNull(recoveryClaim, "recoveryClaim");
        reservedAt = Objects.requireNonNull(reservedAt, "reservedAt");
        deadline = Objects.requireNonNull(deadline, "deadline");
        demandByNode = Map.copyOf(Objects.requireNonNull(demandByNode, "demandByNode"));
        if (recoveryClaim.key().type() != WorkloadClaimType.CLUSTER_RECOVERY
                || recoveryClaim.profileGeneration() < 1
                || !recoveryClaim.key().clusterId().equals(recoveryClaim.key().resourceId())
                || !deadline.isAfter(reservedAt) || demandByNode.isEmpty()
                || demandByNode.keySet().stream().anyMatch(String::isBlank)
                || transferredExecutionGeneration < 0) {
            throw new IllegalArgumentException("recovery permit fields are invalid");
        }
    }

    /** Ownership changes preserve the reservation identity, step and budget instead of allocating again. */
    public ClusterRecoveryPermit handover(WorkloadClaimFence nextClaim) {
        if (!recoveryClaim.key().equals(nextClaim.key())) {
            throw new IllegalArgumentException("handover must retain the recovery resource");
        }
        return new ClusterRecoveryPermit(reservationId, nextClaim, reservedAt, deadline,
                demandByNode, transferredExecutionGeneration);
    }

    public ClusterRecoveryPermit transferred(long executionGeneration) {
        if (executionGeneration < 1 || transferredExecutionGeneration != 0) {
            throw new IllegalArgumentException("reservation can transfer only once to a real execution");
        }
        return new ClusterRecoveryPermit(reservationId, recoveryClaim, reservedAt, deadline,
                demandByNode, executionGeneration);
    }
}
