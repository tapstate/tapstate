package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Majority-durable recovery queue and capacity reservations. Every recovery transition is one atomic
 * conditional operation over the item revision, current intent/incarnation/execution frontier, exact
 * live recovery claim and current profile. A factual failure report instead uses the exact live
 * pipeline claim and preserves the step. The claim/profile documents participate in conditional writes;
 * reading a matching lease followed by an unconditional item write does not implement this contract.
 * All eligibility, backoff, permit and authority deadlines are evaluated on the store clock.
 *
 * <p>The store retains at most one active item and one latest terminal result per incarnation. It does
 * not enumerate live jobs to discover cold-start work, submit jobs, allocate another execution-number
 * sequence or decide whether an ordinary connector failure is a topology failure.
 */
public interface ClusterRecoveryStore {
    int DEFAULT_MAX_CONCURRENT_REBUILDS = 1;

    Optional<ClusterRecoveryItem> read(ClusterRecoveryKey key);

    /** A bounded read of active items and latest terminal results, ordered by enqueue sequence. */
    List<ClusterRecoveryItem> list(String clusterId, int offset, int limit);

    /**
     * Enqueues once under the event key, or returns the active item whose original/successor alias it
     * matches. Without an active item, the original must equal the durable execution frontier and its
     * recorded failure/desired provenance must still authorize this cause. A latest terminal result
     * can be replaced only by a newer genuine event at that frontier; delayed original/successor events
     * cannot erase it. A non-RUNNING, completed, user-terminal or sticky-failed pipeline is not queued.
     */
    Result enqueue(ClusterRecoveryEvent event, ClusterRecoveryFence expected);

    /**
     * Retargets the same active item under the current profile guard, preserving original event,
     * aliases, attempt, permit and allocated successor. The old target is the optimistic item
     * expectation; the new target must match the store's current profile and committed topology.
     */
    Result retarget(ClusterRecoveryFence expected, ClusterExecutionProfile target, long topologyRevision);

    /**
     * Reserves the earliest eligible FIFO item and aggregate per-member capacity in the same atomic
     * operation. Counts live execution demand and valid reservations; unknown computable demand
     * refuses admission. Demand keys must equal the current complete compatible ACTIVE data-member
     * set. Backoff/terminal items do not block later runnable items. No permit issues below the
     * committed majority. A capacity refusal consumes an attempt and applies bounded backoff.
     */
    Result acquirePermit(ClusterRecoveryFence expected, Map<String, ClusterCapacityDemand> demandByNode,
            ClusterCapacityLimits limits, Duration ttl, Duration refusalBackoff, int maxConcurrentRebuilds);

    /**
     * Renews or adopts the same reservation under the new exact recovery claim; does not increment
     * attempt, change successor or allocate another generation. The store sets the deadline, bounded
     * by node/profile authority. Existing execution evidence is observed before any retry is granted.
     */
    Result resumePermit(ClusterRecoveryFence expected, String reservationId, Duration ttl);

    /**
     * Invokes the existing expected-old-generation execution advance and records its successor in
     * this item atomically. Validates permit, intent, item and exact live pipeline claim plus profile;
     * increments attempt only for this real new attempt. An existing successor can be replaced only
     * after its recorded authority is durably fenced and cannot authorize external effects. An owner
     * change resumes that successor instead of automatically calling this operation again.
     */
    Result advanceExecution(ClusterRecoveryFence expected, WorkloadClaim expectedPipelineClaim,
            Set<String> executionNodeIds);

    /** Records the actual frozen compiler selection, including an explicitly known zero-source plan. */
    default Result advanceExecution(ClusterRecoveryFence expected, WorkloadClaim expectedPipelineClaim,
            Set<String> executionNodeIds, Set<String> requiredSourceIds) {
        return new Result(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT, read(expected.key()).orElse(null), null);
    }

    /**
     * Records real submission proof for the matching successor and exact live pipeline claim, moving
     * reservation demand atomically to live execution occupancy keyed by incarnation and generation.
     * Keeps the recovery slot occupied. A normal actuator return without proof does not call this.
     */
    Result recordSubmission(ClusterRecoveryFence expected, WorkloadClaimFence pipelineClaim, String nativeJobId);

    /**
     * Records native initialization and this successor's actual prepared/accepted source facts under
     * its exact pipeline claim/profile fence. The allocated required source set must be complete;
     * original event positions remain diagnostic. A RUNNING checkpoint alone is not proof.
     */
    Result recordStartup(ClusterRecoveryFence expected, WorkloadClaimFence pipelineClaim,
            ClusterRecoveryStartupReceipt receipt);

    /**
     * Completes only from matching startup evidence, or the same successor's successful finite-task
     * completion with that evidence. Releases the recovery slot after live occupancy has transferred;
     * execution demand remains counted while the execution can still authorize work.
     */
    Result complete(ClusterRecoveryFence expected);

    /**
     * Reports a qualified first failure under the allocated successor's exact live pipeline authority.
     * This fact report needs no live recovery holder, preserves permit/demand/attempt, and cannot
     * allocate, release, retry or complete a step. Source facts are consumed in the same transaction;
     * a generic native report requires that execution's durable workload failure verdict. Reporting
     * needs no new data admission quorum and refuses a current completed or user-terminal state.
     */
    default Result recordFailureNote(ClusterRecoveryPipelineFence expected, ClusterRecoveryDiagnostic diagnostic,
            FailureStage stage, CaptureStartupFailure sourceFailure) {
        return new Result(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM, read(expected.key()).orElse(null), null);
    }

    /**
     * Records a bounded failed attempt and releases its reservation only after the exact recorded
     * successor authority is durably fenced. Capacity/pre-allocation refusal consumes one attempt;
     * failure of an allocated execution does not count it twice. Source-position rejection is terminal
     * and preserves the original coded cause and position; no implicit current/earliest/snapshot reset.
     * Exhaustion leaves REBUILD_FAILED visible while releasing FIFO for later runnable items.
     */
    Result fail(ClusterRecoveryFence expected, WorkloadClaimFence pipelineClaim,
            ClusterRecoveryDiagnostic diagnostic, FailureStage stage, Duration backoff);

    /**
     * Cancels when the current incarnation/intent is no longer the queued RUNNING intent. The expected
     * fingerprint matches the item; cancellation deliberately checks the superseding intent instead
     * of requiring it to match. Fences recorded execution authority before releasing any reservation.
     */
    Result cancel(ClusterRecoveryFence expected);

    /**
     * An elapsed permit alone is insufficient: prove the recorded pipeline generation cannot still
     * authorize effects before releasing its slot. Retains the step and attempt for safe resumption;
     * an unknown lease or unreachable store never counts as expiration.
     */
    Result releaseExpiredPermit(ClusterRecoveryFence expected);

    /** Exact-incarnation cleanup is allowed only after resource deletion and all authority is fenced. */
    Result removeDeleted(ClusterRecoveryFence expected);

    enum FailureStage {
        CAPACITY_REFUSAL,
        BEFORE_EXECUTION_ADVANCE,
        ALLOCATED_EXECUTION,
        SOURCE_POSITION_REJECTION
    }

    /** The advanced claim is returned only when its existing allocator committed with the item. */
    record Result(ClusterRecoveryMutation outcome, ClusterRecoveryItem item, WorkloadClaim advancedPipelineClaim) {
        public Result {
            outcome = Objects.requireNonNull(outcome, "outcome");
            if (advancedPipelineClaim != null && (outcome != ClusterRecoveryMutation.APPLIED || item == null
                    || item.successor() == null
                    || !WorkloadClaimFence.from(advancedPipelineClaim).equals(item.successor().pipelineClaim()))) {
                throw new IllegalArgumentException("advanced claim must match the atomically recorded successor");
            }
        }
    }
}
