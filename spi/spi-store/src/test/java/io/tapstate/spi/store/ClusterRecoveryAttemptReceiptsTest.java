package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.model.ReadMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterRecoveryAttemptReceiptsTest {
    private static final Instant NOW = Instant.parse("2026-10-10T01:00:00Z");
    private static final ClusterRecoveryKey KEY = new ClusterRecoveryKey("east", "orders", "inc-a");
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("east", 1,
            new ExecutionProfile(1, Map.of("build", "one")));
    private static final CaptureResumeWitness ORIGINAL = witness(7, ReadMode.CDC_ONLY, List.of("orders"));
    private static final Map<String, ClusterRecoveryPosition> ORIGINAL_POSITIONS = requested(ORIGINAL);

    @Test
    void aSecondAttemptUsesItsOwnAdvancedFloorAfterSnapshotCompletion() {
        ClusterRecoveryPosition snapshot = new ClusterRecoveryPosition("crm", "mongo", "capture-crm",
                ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED, null, "snapshot-incomplete", "original-state");
        ClusterRecoveryItem first = submitted(Map.of("crm", snapshot), Set.of("crm"), 42);
        ClusterRecoveryDiagnostic failed = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), first.event().resumePositions(),
                "Wait for the prior authority to retire");
        ClusterRecoveryItem retry = first.executionFailed(failed, Duration.ofSeconds(1), NOW)
                .permitted(permit("second"), NOW.plusSeconds(1)).advanced(successor(43, Set.of("crm")), NOW.plusSeconds(1))
                .submitted("job-43", NOW.plusSeconds(1));
        CaptureResumeWitness actualStart = witness(19, ReadMode.SNAPSHOT_AND_CDC, List.of("orders"));
        ClusterRecoveryStartupReceipt receipt = receipt(retry, actualStart);

        assertThat(retry.startupReceiptCheck(receipt)).isEqualTo(ClusterRecoveryMutation.APPLIED);
        ClusterRecoveryItem initialized = retry.initialized(receipt, NOW.plusSeconds(1));
        assertThat(initialized.successor().requestedPositions()).isEqualTo(requested(actualStart));
        assertThat(initialized.event().resumePositions()).containsEntry("crm", snapshot);
        assertThat(initialized.recovered(NOW.plusSeconds(1)).executionAliases()).containsExactlyInAnyOrder(41L, 42L, 43L);
    }

    @Test
    void acceptanceWithoutStoredPreparedFactsCannotCompleteAnAttempt() {
        ClusterRecoveryItem item = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryStartupReceipt unproved = new ClusterRecoveryStartupReceipt(item.successor().pipelineClaim(),
                "job-42", NOW, ORIGINAL_POSITIONS, NOW, false);
        assertThat(item.startupReceiptCheck(unproved)).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void archivedSourceSubsetCannotDefineTheTargetExecutionsRequiredSources() {
        ClusterRecoveryItem item = submitted(ORIGINAL_POSITIONS, Set.of("crm", "erp"), 42);
        assertThat(item.startupReceiptCheck(receipt(item, ORIGINAL)))
                .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void aSuccessfullyRecoveredSuccessorCanSufferANewLossAtTheSameCommittedRevision() {
        ClusterRecoveryItem item = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryItem terminal = item.initialized(receipt(item, ORIGINAL), NOW).recovered(NOW);
        ClusterRecoveryEvent nextLoss = event(42, ORIGINAL_POSITIONS, PROFILE, 2);
        assertThat(terminal.terminalEventCheck(nextLoss)).isEqualTo(ClusterRecoveryMutation.APPLIED);
        assertThat(terminal.terminalEventCheck(terminal.event())).isEqualTo(ClusterRecoveryMutation.TERMINAL);
    }

    @Test
    void aNewProfileCannotResetAnExhaustedBudgetAtTheSameIntentAndFrontier() {
        ClusterRecoveryItem failed = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42).executionFailed(
                ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                        new TapstateException(IoError.SRS_PROGRESS_UNPROVEN, Map.of("pipeline", "orders"), null),
                        ORIGINAL_POSITIONS, "Repair the retained position before starting explicitly"), Duration.ofSeconds(1), NOW);
        ClusterExecutionProfile nextProfile = new ClusterExecutionProfile("east", 2, PROFILE.profile());
        assertThat(failed.terminalEventCheck(event(42, ORIGINAL_POSITIONS, nextProfile, 3)))
                .isEqualTo(ClusterRecoveryMutation.TERMINAL);
    }

    @Test
    void aCapacityDiagnosticAcceptsItsRegisteredCanonicalCodeAndNamedParams() {
        ClusterRecoveryDiagnostic diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED,
                new TapstateException(LifecycleError.CLUSTER_CAPACITY_REFUSED,
                        Map.of("node", "a", "resource", "processors", "occupied", 1, "requested", 1, "limit", 1), null),
                ORIGINAL_POSITIONS, "Correct the capacity limit before retrying");
        assertThat(diagnostic.code()).isEqualTo("lifecycle.cluster-capacity-refused");
    }

    @Test
    void onlyAnExplicitlyKnownZeroSourcePlanCanUseAnEmptySourceReceipt() {
        ClusterRecoveryItem known = submitted(ORIGINAL_POSITIONS, Set.of(), 42);
        ClusterRecoveryStartupReceipt nativeOnly = new ClusterRecoveryStartupReceipt(known.successor().pipelineClaim(),
                "job-42", NOW, Map.of(), Map.of(), Map.of(), NOW, false);
        assertThat(known.startupReceiptCheck(nativeOnly)).isEqualTo(ClusterRecoveryMutation.APPLIED);
        assertThat(known.initialized(nativeOnly, NOW).recovered(NOW).status()).isEqualTo(ClusterRecoveryStatus.RECOVERED);
        ClusterRecoverySuccessor unknown = new ClusterRecoverySuccessor(pipeline(42), PROFILE, Set.of("a"), NOW,
                null, null, null);
        ClusterRecoveryItem legacy = ClusterRecoveryItem.enqueued(event(41, ORIGINAL_POSITIONS, PROFILE, 2), 1, NOW, 3)
                .permitted(permit("legacy"), NOW).advanced(unknown, NOW).submitted("job-42", NOW);
        assertThat(legacy.startupReceiptCheck(nativeOnly)).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void anObservedAttemptsPreparedRequestCannotBeReplacedByALaterObservation() {
        ClusterRecoveryItem submitted = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryItem initialized = submitted.initialized(receipt(submitted, ORIGINAL), NOW);
        assertThat(initialized.startupReceiptCheck(receipt(initialized, witness(19, ReadMode.CDC_ONLY, List.of()))))
                .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void aFreshRetryPermitCannotCompleteUsingTheRetiredSuccessorsOldReceipt() {
        ClusterRecoveryItem submitted = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryItem initialized = submitted.initialized(receipt(submitted, ORIGINAL), NOW);
        ClusterRecoveryDiagnostic failed = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), ORIGINAL_POSITIONS,
                "Wait for the prior authority to retire");
        ClusterRecoveryItem retry = initialized.executionFailed(failed, Duration.ofSeconds(1), NOW)
                .permitted(permit("second"), NOW.plusSeconds(1));
        assertThat(retry.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void anOriginalAuthorityWaitLeavesLaterRunnableWorkEligibleWithoutConsumingRetryBudget() {
        ClusterRecoveryItem head = ClusterRecoveryItem.enqueued(event(41, ORIGINAL_POSITIONS, PROFILE, 2), 1, NOW, 3);
        ClusterRecoveryItem later = ClusterRecoveryItem.enqueued(event(41, ORIGINAL_POSITIONS, PROFILE, 2), 2, NOW, 3);
        ClusterRecoveryItem waiting = head.deferredUntil(NOW.plusSeconds(20), NOW);
        assertThat(waiting.eligibleAt(NOW)).isFalse();
        assertThat(later.eligibleAt(NOW)).isTrue();
        assertThat(waiting.eligibleAt(NOW.plusSeconds(20))).isTrue();
        assertThat(waiting.attempt()).isZero();
        assertThat(waiting.executionAliases()).containsExactly(41L);
    }

    @Test
    void aFirstFailureNoteRetainsOccupancyAndSurvivesOwnerHandover() {
        ClusterRecoveryItem item = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryDiagnostic rejection = rejection();
        var note = new ClusterRecoveryFailureNote(item.successor().pipelineClaim(),
                ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, rejection, NOW);
        ClusterRecoveryItem recorded = item.failureNoted(note, NOW);
        assertThat(recorded.permit()).isEqualTo(item.permit());
        assertThat(recorded.attempt()).isEqualTo(item.attempt());
        assertThat(recorded.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        ClusterRecoveryItem inherited = recorded.permitted(recorded.permit().handover(new WorkloadClaimFence(
                recorded.permit().recoveryClaim().key(), new WorkloadOwner("b", "next-boot"), 2, 0, 2, 1)), NOW);
        ClusterRecoveryDiagnostic placeholder = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), ORIGINAL_POSITIONS, "Wait for retirement");
        ClusterRecoveryItem terminal = inherited.executionFailed(placeholder, Duration.ofSeconds(1), NOW);
        assertThat(terminal.status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
        assertThat(terminal.diagnostic()).isEqualTo(rejection);
        assertThat(terminal.attempt()).isEqualTo(1);
    }

    @Test
    void aLaterFailureCannotOverwriteTheFirstAndAnOldReceiptCannotRecoverIt() {
        ClusterRecoveryItem submitted = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryItem initialized = submitted.initialized(receipt(submitted, ORIGINAL), NOW);
        var first = new ClusterRecoveryFailureNote(initialized.successor().pipelineClaim(),
                ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, rejection(), NOW);
        ClusterRecoveryItem noted = initialized.failureNoted(first, NOW);
        var later = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), ORIGINAL_POSITIONS, "Wait for retirement");
        assertThat(noted.failureNoted(new ClusterRecoveryFailureNote(first.pipelineClaim(),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, later, NOW.plusSeconds(1)),
                NOW.plusSeconds(1))).isEqualTo(noted);
        assertThat(noted.startupReceiptCheck(receipt(noted, ORIGINAL))).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        assertThat(noted.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void aRealNewAttemptClearsThePriorFailureNoteAndCanProveItsOwnStartup() {
        ClusterRecoveryItem submitted = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        ClusterRecoveryDiagnostic error = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), ORIGINAL_POSITIONS, "Retry after retirement");
        ClusterRecoveryItem failed = submitted.failureNoted(new ClusterRecoveryFailureNote(submitted.successor().pipelineClaim(),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, error, NOW), NOW)
                .executionFailed(error, Duration.ofSeconds(1), NOW);
        ClusterRecoveryItem next = failed.permitted(permit("second"), NOW.plusSeconds(1))
                .advanced(successor(43, Set.of("crm")), NOW.plusSeconds(1)).submitted("job-43", NOW.plusSeconds(1));
        assertThat(next.startupReceiptCheck(receipt(next, ORIGINAL))).isEqualTo(ClusterRecoveryMutation.APPLIED);
        assertThat(next.initialized(receipt(next, ORIGINAL), NOW.plusSeconds(1)).recovered(NOW.plusSeconds(1)).status())
                .isEqualTo(ClusterRecoveryStatus.RECOVERED);
    }

    @Test
    void allocatorBindingStillNeedsIndependentSubmissionAndStartupFacts() {
        ClusterRecoveryItem allocated = ClusterRecoveryItem.enqueued(event(41, ORIGINAL_POSITIONS, PROFILE, 2), 1, NOW, 3)
                .permitted(permit("first"), NOW).advanced(successor(42, Set.of("crm")), NOW);
        assertThat(allocated.hasAllocatedSuccessor()).isTrue();
        assertThat(allocated.permit().transferredExecutionGeneration()).isEqualTo(42);
        assertThat(allocated.successor().nativeJobId()).isNull();
        assertThat(allocated.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        var submitted = allocated.submitted("job-42", NOW);
        assertThat(submitted.hasAllocatedSuccessor()).isTrue();
        assertThat(submitted.attempt()).isEqualTo(1);
        assertThat(submitted.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void aNewPermitRetainsHistoryWithoutAcceptingItsExecutionFacts() {
        ClusterRecoveryItem original = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        var oldCause = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), ORIGINAL_POSITIONS, "Wait for retirement");
        var note = new ClusterRecoveryFailureNote(original.successor().pipelineClaim(),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, oldCause, NOW);
        var fresh = original.failureNoted(note, NOW).executionFailed(oldCause, Duration.ofSeconds(1), NOW)
                .permitted(permit("second"), NOW.plusSeconds(1));
        assertThat(fresh.successor().failureNote()).isEqualTo(note);
        assertThat(fresh.hasAllocatedSuccessor()).isFalse();
        assertThat(fresh.startupReceiptCheck(receipt(original, ORIGINAL))).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        assertThat(fresh.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        assertThatThrownBy(() -> fresh.failureNoted(note, NOW.plusSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fresh.executionFailed(oldCause, Duration.ofSeconds(1), NOW.plusSeconds(1)))
                .isInstanceOf(IllegalStateException.class);
        var currentCause = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(LifecycleError.PIPELINE_NOT_RUNNABLE, Map.of("pipeline", "orders"), null),
                ORIGINAL_POSITIONS, "Correct the current compilation refusal");
        var refused = fresh.refused(currentCause, Duration.ofSeconds(1), NOW.plusSeconds(1));
        assertThat(refused.attempt()).isEqualTo(2);
        assertThat(refused.diagnostic()).isEqualTo(currentCause);
        assertThat(refused.successor().failureNote()).isEqualTo(note);
    }

    @Test
    void aCompatibleTargetAdvanceKeepsTheOriginalSuccessfulExecutionAndSourceReceipt() {
        var original = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        var retargeted = original.retargeted(PROFILE, 3, NOW);
        var completed = retargeted.initialized(receipt(original, ORIGINAL), NOW).recovered(NOW);
        assertThat(completed.targetTopologyRevision()).isEqualTo(3);
        assertThat(completed.successor().pipelineClaim().topologyRevision()).isEqualTo(2);
        assertThat(completed.successor().executionNodeIds()).isEqualTo(original.successor().executionNodeIds());
        assertThat(completed.successor().startupReceipt()).isEqualTo(receipt(original, ORIGINAL));
        assertThat(completed.attempt()).isEqualTo(1);
    }

    @Test
    void aNewProfileCannotCompleteUsingAnOlderProfilesStartupProof() {
        var original = submitted(ORIGINAL_POSITIONS, Set.of("crm"), 42);
        var next = new ClusterExecutionProfile("east", 2, PROFILE.profile());
        var incompatible = original.retargeted(next, 3, NOW).initialized(receipt(original, ORIGINAL), NOW);
        assertThat(incompatible.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        assertThatThrownBy(() -> incompatible.recovered(NOW)).isInstanceOf(IllegalStateException.class);
    }

    private static ClusterRecoveryDiagnostic rejection() {
        return ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                new TapstateException(IoError.SRS_PROGRESS_UNPROVEN, Map.of("pipeline", "orders"), null),
                ORIGINAL_POSITIONS, "Restore the retained source position before starting explicitly");
    }

    private static ClusterRecoveryItem submitted(Map<String, ClusterRecoveryPosition> original, Set<String> required, long execution) {
        return ClusterRecoveryItem.enqueued(event(41, original, PROFILE, 2), 1, NOW, 3)
                .permitted(permit("first"), NOW).advanced(successor(execution, required), NOW).submitted("job-" + execution, NOW);
    }

    private static ClusterRecoveryEvent event(long execution, Map<String, ClusterRecoveryPosition> original,
            ClusterExecutionProfile target, long topology) {
        return new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.MEMBER_LOSS, execution, "revision", 1L,
                PROFILE, target, topology, "intent", original);
    }

    private static ClusterRecoverySuccessor successor(long execution, Set<String> required) {
        return new ClusterRecoverySuccessor(pipeline(execution), PROFILE, Set.of("a"), required, NOW,
                null, null, Map.of(), null);
    }

    private static ClusterRecoveryPermit permit(String id) {
        WorkloadClaimFence recovery = new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CLUSTER_RECOVERY, "east"),
                new WorkloadOwner("a", "boot-a"), 1, 0, 2, 1);
        return new ClusterRecoveryPermit(id, recovery, NOW, NOW.plusSeconds(30),
                Map.of("a", new ClusterCapacityDemand(1, 0, 1, 1, 1, 1)), 0);
    }

    private static WorkloadClaimFence pipeline(long execution) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("a", "boot-a"), 1, execution, 2, 1);
    }

    private static CaptureResumeWitness witness(long sequence, ReadMode mode, List<String> completedTables) {
        ChainPosition floor = new ChainPosition(new SourceOrder(7, sequence), "resume-" + sequence);
        return new CaptureResumeWitness("crm", "mongo", "capture-crm", SrsConsumerId.of("orders", "crm").value(), mode,
                true, List.of("orders"), true, 7, floor, true, true, completedTables, "seam-3", 7,
                ConsumerProgressKind.SRS, floor, Map.of("orders", floor));
    }

    private static Map<String, ClusterRecoveryPosition> requested(CaptureResumeWitness witness) {
        return Map.of(witness.sourceId(), witness.requestedPosition(witness.miningChainId()).orElseThrow());
    }

    private static ClusterRecoveryStartupReceipt receipt(ClusterRecoveryItem item, CaptureResumeWitness witness) {
        return new ClusterRecoveryStartupReceipt(item.successor().pipelineClaim(), item.successor().nativeJobId(), NOW,
                Map.of(witness.sourceId(), witness), requested(witness), requested(witness), NOW, false);
    }
}
