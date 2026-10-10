package io.tapstate.spi.store;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Versioned durable recovery step. Pure transformations do not authorize persistence: adapters apply
 * them only inside the claim, profile, intent, frontier and item-revision conditions of the store port.
 */
public record ClusterRecoveryItem(
        int schemaVersion, ClusterRecoveryEvent event, long itemRevision, long enqueueSequence,
        Instant enqueuedAt, Instant updatedAt, ClusterExecutionProfile targetProfile,
        long targetTopologyRevision, ClusterRecoveryStatus status, int attempt, int maxAttempts,
        Instant nextEligibleAt, Set<Long> executionAliases, ClusterRecoveryPermit permit,
        ClusterRecoverySuccessor successor, ClusterRecoveryDiagnostic diagnostic) {

    public static final int SCHEMA_VERSION = 1;
    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    public ClusterRecoveryItem {
        event = Objects.requireNonNull(event, "event");
        enqueuedAt = Objects.requireNonNull(enqueuedAt, "enqueuedAt");
        updatedAt = Objects.requireNonNull(updatedAt, "updatedAt");
        targetProfile = Objects.requireNonNull(targetProfile, "targetProfile");
        status = Objects.requireNonNull(status, "status");
        executionAliases = Set.copyOf(Objects.requireNonNull(executionAliases, "executionAliases"));
        if (schemaVersion != SCHEMA_VERSION || itemRevision < 1 || enqueueSequence < 1
                || targetTopologyRevision < 1 || attempt < 0 || maxAttempts < 1 || attempt > maxAttempts
                || targetProfile.generation() < event.targetProfile().generation()
                || !targetProfile.clusterId().equals(event.key().clusterId())
                || !executionAliases.contains(event.originalExecutionGeneration())
                || executionAliases.stream().anyMatch(generation -> generation < 1)
                || executionAliases.size() > (long) maxAttempts + 1 || updatedAt.isBefore(enqueuedAt)
                || (status.terminal() && permit != null)
                || (status == ClusterRecoveryStatus.RETRY_BACKOFF && nextEligibleAt == null)
                || (status == ClusterRecoveryStatus.REBUILDING && permit == null)
                || (status == ClusterRecoveryStatus.REBUILD_FAILED && diagnostic == null)
                || (permit != null && status != ClusterRecoveryStatus.REBUILDING)) {
            throw new IllegalArgumentException("recovery item identity, revision or step is invalid");
        }
        if (successor != null && (!executionAliases.contains(successor.executionGeneration())
                || !successor.pipelineClaim().key().clusterId().equals(event.key().clusterId())
                || !successor.pipelineClaim().key().resourceId().equals(event.key().pipelineId())
                || successor.executionGeneration() <= event.originalExecutionGeneration() || attempt == 0)) {
            throw new IllegalArgumentException("successor must belong to this item's execution aliases");
        }
        if (permit != null && (permit.transferredExecutionGeneration() != 0
                && (successor == null || permit.transferredExecutionGeneration() != successor.executionGeneration()))) {
            throw new IllegalArgumentException("transferred reservation must match the successor");
        }
        if (status == ClusterRecoveryStatus.RECOVERED && (!hasMatchingStartup(event, successor)
                || !targetProfile.equals(successor.profile())
                || targetTopologyRevision != successor.pipelineClaim().topologyRevision())) {
            throw new IllegalArgumentException("recovered item requires matching initialization and source acceptance");
        }
    }

    public static ClusterRecoveryItem enqueued(ClusterRecoveryEvent event, long sequence,
            Instant storeTime, int maxAttempts) {
        return new ClusterRecoveryItem(SCHEMA_VERSION, event, 1, sequence, storeTime, storeTime,
                event.targetProfile(), event.targetTopologyRevision(), ClusterRecoveryStatus.WAITING_PERMIT,
                0, maxAttempts, storeTime, Set.of(event.originalExecutionGeneration()), null, null, null);
    }

    /** A successor found by a cold scan resumes this item instead of becoming another queue entry. */
    public boolean matchesActiveAlias(ClusterRecoveryEvent candidate) {
        return !status.terminal() && event.key().equals(candidate.key())
                && event.intentFingerprint().equals(candidate.intentFingerprint())
                && executionAliases.contains(candidate.originalExecutionGeneration());
    }

    public long executionFrontier() {
        return executionAliases.stream().mapToLong(Long::longValue).max().orElseThrow();
    }

    /** Pure optimistic checks; stores additionally condition-write the live claims and profile guard. */
    public ClusterRecoveryMutation check(ClusterRecoveryFence expected, String currentIntentFingerprint,
            ClusterExecutionProfile currentProfile, long currentExecutionGeneration) {
        if (!event.key().equals(expected.key()) || itemRevision != expected.itemRevision()) {
            return ClusterRecoveryMutation.STALE_ITEM;
        }
        if (status.terminal()) {
            return ClusterRecoveryMutation.TERMINAL;
        }
        if (!event.intentFingerprint().equals(expected.intentFingerprint())
                || !event.intentFingerprint().equals(currentIntentFingerprint)) {
            return ClusterRecoveryMutation.STALE_INTENT;
        }
        if (!targetProfile.equals(expected.targetProfile()) || !targetProfile.equals(currentProfile)) {
            return ClusterRecoveryMutation.STALE_PROFILE;
        }
        if (executionFrontier() != expected.executionGeneration()
                || executionFrontier() != currentExecutionGeneration) {
            return ClusterRecoveryMutation.STALE_EXECUTION;
        }
        return ClusterRecoveryMutation.APPLIED;
    }

    public boolean eligibleAt(Instant storeTime) {
        return (status == ClusterRecoveryStatus.WAITING_PERMIT || status == ClusterRecoveryStatus.RETRY_BACKOFF)
                && permit == null
                && (nextEligibleAt == null || !storeTime.isBefore(nextEligibleAt));
    }

    /** Retargeting never discards an in-flight successor or its already consumed attempt. */
    public ClusterRecoveryItem retargeted(ClusterExecutionProfile profile, long topologyRevision, Instant storeTime) {
        requireActive();
        if (profile.generation() < targetProfile.generation() || topologyRevision < targetTopologyRevision) {
            throw new IllegalArgumentException("recovery target generations cannot move backwards");
        }
        return copy(profile, topologyRevision, status, attempt, nextEligibleAt,
                executionAliases, permit, successor, diagnostic, storeTime);
    }

    /** An adopted permit keeps the same durable step; adoption is not another recovery attempt. */
    public ClusterRecoveryItem permitted(ClusterRecoveryPermit nextPermit, Instant storeTime) {
        requireActive();
        Objects.requireNonNull(nextPermit, "nextPermit");
        if (!event.key().clusterId().equals(nextPermit.recoveryClaim().key().clusterId())
                || (permit != null && (!permit.reservationId().equals(nextPermit.reservationId())
                || !permit.demandByNode().equals(nextPermit.demandByNode())
                || !permit.reservedAt().equals(nextPermit.reservedAt())
                || permit.transferredExecutionGeneration() != nextPermit.transferredExecutionGeneration()))) {
            throw new IllegalArgumentException("permit adoption must retain the reservation and execution");
        }
        return copy(targetProfile, targetTopologyRevision, ClusterRecoveryStatus.REBUILDING, attempt,
                null, executionAliases, nextPermit, successor, diagnostic, storeTime);
    }

    /** Called only after the existing execution allocator and this receipt commit atomically. */
    public ClusterRecoveryItem advanced(ClusterRecoverySuccessor nextSuccessor, Instant storeTime) {
        requireActive();
        if (permit == null || permit.transferredExecutionGeneration() != 0 || attempt >= maxAttempts
                || nextSuccessor.executionGeneration() != Math.addExact(executionFrontier(), 1)
                || !targetProfile.equals(nextSuccessor.profile())
                || !permit.demandByNode().keySet().equals(nextSuccessor.executionNodeIds())) {
            throw new IllegalArgumentException("execution advance must consume the exact untransferred permit");
        }
        Set<Long> aliases = new HashSet<>(executionAliases);
        aliases.add(nextSuccessor.executionGeneration());
        return copy(targetProfile, targetTopologyRevision, ClusterRecoveryStatus.REBUILDING, attempt + 1,
                null, aliases, permit, nextSuccessor, diagnostic, storeTime);
    }

    /** Submission proof transfers reservation occupancy without opening another recovery slot. */
    public ClusterRecoveryItem submitted(String nativeJobId, Instant storeTime) {
        requireActive();
        if (permit == null || successor == null || successor.submittedAt() != null) {
            throw new IllegalStateException("submission requires an unsubmitted allocated successor");
        }
        ClusterRecoverySuccessor submitted = new ClusterRecoverySuccessor(successor.pipelineClaim(),
                successor.profile(), successor.executionNodeIds(), successor.allocatedAt(),
                nativeJobId, storeTime, null);
        return copy(targetProfile, targetTopologyRevision, status, attempt, null, executionAliases,
                permit.transferred(successor.executionGeneration()), submitted, diagnostic, storeTime);
    }

    public ClusterRecoveryMutation startupReceiptCheck(ClusterRecoveryStartupReceipt receipt) {
        if (successor == null || !successor.pipelineClaim().equals(receipt.pipelineClaim())
                || !Objects.equals(successor.nativeJobId(), receipt.nativeJobId())) {
            return ClusterRecoveryMutation.STALE_EXECUTION;
        }
        return event.resumePositions().equals(receipt.acceptedPositions())
                ? ClusterRecoveryMutation.APPLIED : ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT;
    }

    public ClusterRecoveryItem initialized(ClusterRecoveryStartupReceipt receipt, Instant storeTime) {
        requireActive();
        if (startupReceiptCheck(receipt) != ClusterRecoveryMutation.APPLIED || permit == null
                || permit.transferredExecutionGeneration() != successor.executionGeneration()) {
            throw new IllegalArgumentException("startup receipt must match the transferred successor and original positions");
        }
        ClusterRecoverySuccessor initialized = new ClusterRecoverySuccessor(successor.pipelineClaim(),
                successor.profile(), successor.executionNodeIds(), successor.allocatedAt(),
                successor.nativeJobId(), successor.submittedAt(), receipt);
        return copy(targetProfile, targetTopologyRevision, status, attempt, null, executionAliases,
                permit, initialized, diagnostic, storeTime);
    }

    public ClusterRecoveryMutation completionCheck() {
        if (status.terminal()) {
            return ClusterRecoveryMutation.TERMINAL;
        }
        return status == ClusterRecoveryStatus.REBUILDING && permit != null
                && hasMatchingStartup(event, successor) && targetProfile.equals(successor.profile())
                && targetTopologyRevision == successor.pipelineClaim().topologyRevision()
                ? ClusterRecoveryMutation.APPLIED : ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT;
    }

    public ClusterRecoveryItem recovered(Instant storeTime) {
        if (completionCheck() != ClusterRecoveryMutation.APPLIED) {
            throw new IllegalStateException("recovery requires matching native and source startup evidence");
        }
        return copy(targetProfile, targetTopologyRevision, ClusterRecoveryStatus.RECOVERED, attempt,
                null, executionAliases, null, successor, diagnostic, storeTime);
    }

    /** Called only after the store has fenced the old execution and its reservation. */
    public ClusterRecoveryItem cancelled(Instant storeTime) {
        requireActive();
        return copy(targetProfile, targetTopologyRevision, ClusterRecoveryStatus.CANCELLED, attempt,
                null, executionAliases, null, successor, diagnostic, storeTime);
    }

    /** Capacity or pre-allocation refusal consumes a bounded attempt without allocating an execution. */
    public ClusterRecoveryItem refused(ClusterRecoveryDiagnostic cause, Duration backoff, Instant storeTime) {
        requireActive();
        if (attempt >= maxAttempts) {
            throw new IllegalStateException("the retry budget is exhausted");
        }
        return failed(cause, attempt + 1, backoff, storeTime);
    }

    /** A previously allocated failed attempt is counted once; its generation remains visible. */
    public ClusterRecoveryItem executionFailed(ClusterRecoveryDiagnostic cause, Duration backoff, Instant storeTime) {
        requireActive();
        if (successor == null) {
            throw new IllegalStateException("execution failure requires an allocated successor");
        }
        return failed(cause, attempt, backoff, storeTime);
    }

    private ClusterRecoveryItem failed(ClusterRecoveryDiagnostic cause, int usedAttempts,
            Duration backoff, Instant storeTime) {
        Objects.requireNonNull(cause, "cause");
        Objects.requireNonNull(backoff, "backoff");
        if (backoff.isNegative() || backoff.isZero()) {
            throw new IllegalArgumentException("retry backoff must be positive");
        }
        boolean terminal = usedAttempts >= maxAttempts
                || cause.reason() == ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED;
        return copy(targetProfile, targetTopologyRevision,
                terminal ? ClusterRecoveryStatus.REBUILD_FAILED : ClusterRecoveryStatus.RETRY_BACKOFF,
                usedAttempts, terminal ? null : storeTime.plus(backoff), executionAliases,
                null, successor, cause, storeTime);
    }

    private ClusterRecoveryItem copy(ClusterExecutionProfile profile, long topologyRevision,
            ClusterRecoveryStatus nextStatus, int nextAttempt, Instant eligible, Set<Long> aliases,
            ClusterRecoveryPermit nextPermit, ClusterRecoverySuccessor nextSuccessor,
            ClusterRecoveryDiagnostic nextDiagnostic, Instant storeTime) {
        return new ClusterRecoveryItem(schemaVersion, event, Math.addExact(itemRevision, 1), enqueueSequence,
                enqueuedAt, storeTime, profile, topologyRevision, nextStatus, nextAttempt, maxAttempts,
                eligible, aliases, nextPermit, nextSuccessor, nextDiagnostic);
    }

    private void requireActive() {
        if (status.terminal()) {
            throw new IllegalStateException("terminal recovery items cannot transition");
        }
    }

    private static boolean hasMatchingStartup(ClusterRecoveryEvent event, ClusterRecoverySuccessor successor) {
        return successor != null && successor.startupReceipt() != null
                && event.resumePositions().equals(successor.startupReceipt().acceptedPositions());
    }
}
