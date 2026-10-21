package io.tapstate.control.core;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Durable recovery facts for one current resource incarnation, independent of HTTP readiness. */
public record ClusterRecoveryItemView(
        String pipelineId, String incarnation, String currentArtifactHash, String intentFingerprint,
        String cause, String persistedStatus, String status,
        long enqueueSequence, Instant enqueuedAt, Instant updatedAt, Integer queuePosition,
        int attempt, int maxAttempts, Instant nextEligibleAt,
        long originalExecutionGeneration, String originalExecutionRevision, Long sourceTopologyRevision,
        Profile originalProfile, boolean legacySourceProfile, Profile targetProfile,
        long targetTopologyRevision, long executionFrontier,
        Map<String, Position> originalPositions, Permit permit, Successor successor, Diagnostic diagnostic,
        ClaimReading currentPipelineClaim, ClusterRecoveryReadFailure claimUnavailable) {
    public ClusterRecoveryItemView {
        originalPositions = immutableMap(originalPositions);
    }

    public record Profile(long generation, int formatVersion, String hash, Map<String, String> attributes) {
        public Profile {
            attributes = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(attributes, "attributes")));
        }
    }

    /** Claim and execution generations remain distinct from topology and profile generations. */
    public record Claim(String clusterId, String type, String resourceId, String ownerNodeId, String ownerBootId,
            long claimGeneration, long executionGeneration, long topologyRevision, long profileGeneration) {}

    /** Lease liveness comes from the store clock; a retained owner record alone never proves liveness. */
    public record ClaimReading(Claim claim, Instant leaseUntil, long leaseRemainingMillis, boolean leased) {}

    public record Permit(String reservationId, Instant reservedAt, Instant deadline, Claim coordinator,
            long transferredExecutionGeneration, Map<String, ClusterResourceCounts> demandByNode) {
        public Permit { demandByNode = immutableMap(demandByNode); }
    }

    public record Successor(Claim pipelineClaim, Profile profile, List<String> executionNodeIds,
            List<String> requiredSourceIds, boolean sourceRequirementsRecorded, Instant allocatedAt,
            String nativeJobId, Instant submittedAt, Instant nativeInitializedAt, Instant sourcesAcceptedAt,
            Boolean executionCompleted, Map<String, Position> requestedPositions, Map<String, Position> acceptedPositions,
            FailureNote failureNote) {
        public Successor {
            executionNodeIds = List.copyOf(executionNodeIds);
            requiredSourceIds = List.copyOf(requiredSourceIds);
            requestedPositions = immutableMap(requestedPositions);
            acceptedPositions = immutableMap(acceptedPositions);
        }

        public Successor(Claim pipelineClaim, Profile profile, List<String> executionNodeIds,
                List<String> requiredSourceIds, boolean sourceRequirementsRecorded, Instant allocatedAt,
                String nativeJobId, Instant submittedAt, Instant nativeInitializedAt, Instant sourcesAcceptedAt,
                Boolean executionCompleted, Map<String, Position> requestedPositions, Map<String, Position> acceptedPositions) {
            this(pipelineClaim, profile, executionNodeIds, requiredSourceIds, sourceRequirementsRecorded, allocatedAt,
                    nativeJobId, submittedAt, nativeInitializedAt, sourcesAcceptedAt, executionCompleted,
                    requestedPositions, acceptedPositions, null);
        }
    }

    /** The current successor's first qualified failure, retained while its old authority retires. */
    public record FailureNote(Claim pipelineClaim, String stage, Diagnostic diagnostic, Instant recordedAt) {}

    /** A source's exact archived or attempted point, with an honest absence of an order or token. */
    public record Position(String connectorId, String captureId, String kind, Long epoch, Long sequence,
            String token, String provenance, String reference) {}

    public record Diagnostic(String reason, String code, Map<String, Object> params,
            Map<String, Position> positions, String disposition) {
        public Diagnostic {
            params = immutableMap(params);
            positions = immutableMap(positions);
        }
    }

    private static <T> Map<String, T> immutableMap(Map<String, T> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, "values")));
    }
}
