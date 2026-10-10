package io.tapstate.cli;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Recovery wire facts mirrored independently on the HTTP surface. */
final class RemoteRecovery {
    private RemoteRecovery() {}

    record Cluster(String clusterId, Boolean quorumReady, String recoveryState, List<String> causes,
            List<Item> items, Profile currentProfile, ReadFailure profileUnavailable,
            ClaimReading coordinatorClaim, ReadFailure claimUnavailable, Capacity capacity,
            ReadFailure queueUnavailable) {
        Cluster { causes = List.copyOf(causes); items = List.copyOf(items); }
    }

    record Pipeline(String pipelineId, String currentIncarnation, String recoveryState,
            List<String> causes, List<Item> items, ReadFailure unavailable, Capacity capacity) {
        Pipeline { causes = List.copyOf(causes); items = List.copyOf(items); }
    }

    record Item(String pipelineId, String incarnation, String currentArtifactHash, String intentFingerprint,
            String cause, String persistedStatus, String status, Long enqueueSequence, String enqueuedAt,
            String updatedAt, Integer queuePosition, Integer attempt, Integer maxAttempts, String nextEligibleAt,
            Long originalExecutionGeneration, String originalExecutionRevision, Long sourceTopologyRevision,
            Profile originalProfile, Boolean legacySourceProfile, Profile targetProfile, Long targetTopologyRevision,
            Long executionFrontier, Map<String, Position> originalPositions, Permit permit, Successor successor,
            Diagnostic diagnostic, ClaimReading currentPipelineClaim, ReadFailure claimUnavailable) {
        Item { originalPositions = frozen(originalPositions); }
    }

    record Profile(Long generation, Integer formatVersion, String hash, Map<String, String> attributes) {
        Profile { attributes = frozen(attributes); }
    }

    record Claim(String clusterId, String type, String resourceId, String ownerNodeId, String ownerBootId,
            Long claimGeneration, Long executionGeneration, Long topologyRevision, Long profileGeneration) {}

    record ClaimReading(Claim claim, String leaseUntil, Long leaseRemainingMillis, Boolean leased) {}

    record Permit(String reservationId, String reservedAt, String deadline, Claim coordinator,
            Long transferredExecutionGeneration, Map<String, Counts> demandByNode) {
        Permit { demandByNode = frozen(demandByNode); }
    }

    record Successor(Claim pipelineClaim, Profile profile, List<String> executionNodeIds,
            List<String> requiredSourceIds, Boolean sourceRequirementsRecorded, String allocatedAt,
            String nativeJobId, String submittedAt, String nativeInitializedAt, String sourcesAcceptedAt,
            Boolean executionCompleted, Map<String, Position> requestedPositions,
            Map<String, Position> acceptedPositions, FailureNote failureNote) {
        Successor {
            executionNodeIds = List.copyOf(executionNodeIds);
            requiredSourceIds = List.copyOf(requiredSourceIds);
            requestedPositions = frozen(requestedPositions);
            acceptedPositions = frozen(acceptedPositions);
        }
    }

    record FailureNote(Claim pipelineClaim, String stage, Diagnostic diagnostic, String recordedAt) {}

    record Position(String connectorId, String captureId, String kind, Long epoch, Long sequence,
            String token, String provenance, String reference) {}

    record Diagnostic(String reason, String code, Map<String, Object> params,
            Map<String, Position> positions, String disposition) {
        Diagnostic { params = frozen(params); positions = frozen(positions); }
    }

    record Capacity(String availability, String provenance, Profile profile, Counts configuredLimits,
            Map<String, Counts> occupiedByNode, ReadFailure unavailable) {
        Capacity { occupiedByNode = occupiedByNode == null ? null : frozen(occupiedByNode); }
    }

    record Counts(Long processors, Long blockingProcessors, Long writers, Long connectorInstances,
            Long bufferedRecords, Long edgeQueueRecords) {}

    record ReadFailure(String code, Map<String, Object> params) {
        ReadFailure { params = frozen(params); }
    }

    private static <T> Map<String, T> frozen(Map<String, T> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
