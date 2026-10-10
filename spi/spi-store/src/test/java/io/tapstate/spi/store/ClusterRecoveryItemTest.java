package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterRecoveryItemTest {
    private static final Instant NOW = Instant.parse("2026-10-10T01:00:00Z");
    private static final ClusterRecoveryKey KEY = new ClusterRecoveryKey("east", "orders", "incarnation-a");
    private static final ClusterExecutionProfile PROFILE = profile(1, "build-a");
    private static final Map<String, ClusterRecoveryPosition> POSITIONS = Map.of("crm",
            new ClusterRecoveryPosition("crm", "mongo", "capture-orders", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                    new ChainPosition(new SourceOrder(7, 93), "resume-93"), "capture-majority-read", "srs/crm/7"));
    private static final Map<String, ClusterCapacityDemand> DEMAND = Map.of("node-a",
            new ClusterCapacityDemand(3, 1, 1, 2, 200, 400));
    private static final String INTENT = fingerprint(PipelineState.RUNNING);

    @Test
    void staleItemIntentProfileAndExecutionAreDifferentRefusals() {
        ClusterRecoveryItem item = queued();
        ClusterRecoveryFence current = fence(item, recoveryClaim(1));

        assertThat(item.check(current, INTENT, PROFILE, 41)).isEqualTo(ClusterRecoveryMutation.APPLIED);
        assertThat(item.check(new ClusterRecoveryFence(KEY, 2, INTENT, PROFILE, 41, recoveryClaim(1)),
                INTENT, PROFILE, 41)).isEqualTo(ClusterRecoveryMutation.STALE_ITEM);
        assertThat(item.check(current, fingerprint(PipelineState.PAUSED), PROFILE, 41))
                .isEqualTo(ClusterRecoveryMutation.STALE_INTENT);
        assertThat(item.check(current, INTENT, profile(2, "build-a"), 41))
                .isEqualTo(ClusterRecoveryMutation.STALE_PROFILE);
        assertThat(item.check(current, INTENT, profile(1, "build-b"), 41))
                .isEqualTo(ClusterRecoveryMutation.STALE_PROFILE);
        assertThat(item.check(current, INTENT, PROFILE, 42)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        assertThat(item.check(new ClusterRecoveryFence(KEY, 1, INTENT, PROFILE, 42, recoveryClaim(1)),
                INTENT, PROFILE, 41)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        ClusterRecoveryKey recreated = new ClusterRecoveryKey("east", "orders", "incarnation-b");
        assertThat(item.check(new ClusterRecoveryFence(recreated, 1, INTENT, PROFILE, 41, recoveryClaim(1)),
                INTENT, PROFILE, 41)).isEqualTo(ClusterRecoveryMutation.STALE_ITEM);
    }

    @Test
    void coldScansAndRepeatedMemberLossMatchOriginalAndSuccessorAliases() {
        ClusterRecoveryItem item = allocated();
        ClusterRecoveryEvent successorScan = event(KEY, 42, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                profile(2, "build-a"), 3, INTENT, POSITIONS);

        assertThat(item.matchesActiveAlias(event())).isTrue();
        assertThat(item.matchesActiveAlias(successorScan)).isTrue();
        assertThat(item.matchesActiveAlias(event(KEY, 43, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 2, INTENT, POSITIONS))).isFalse();
        assertThat(item.matchesActiveAlias(event(new ClusterRecoveryKey("east", "orders", "incarnation-b"),
                42, ClusterRecoveryCause.MEMBER_LOSS, PROFILE, 2, INTENT, POSITIONS))).isFalse();
        assertThat(item.matchesActiveAlias(event(KEY, 42, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 2, fingerprint(PipelineState.STOPPED), POSITIONS))).isFalse();

        ClusterRecoveryItem retargeted = item.retargeted(profile(2, "build-a"), 3, NOW.plusSeconds(1));
        assertThat(retargeted.event().uniqueKey()).isEqualTo(item.event().uniqueKey());
        assertThat(retargeted.successor()).isEqualTo(item.successor());
        assertThat(retargeted.executionAliases()).containsExactlyInAnyOrder(41L, 42L);
        assertThat(retargeted.attempt()).isEqualTo(1);
    }

    @Test
    void eventIdentityUsesTopologyForMemberLossAndProfileForFullRestart() {
        assertThat(event().uniqueKey()).isNotEqualTo(event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 3, INTENT, POSITIONS).uniqueKey());
        ClusterRecoveryEvent restart = event(KEY, 41, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                profile(2, "build-a"), 3, INTENT, POSITIONS);
        assertThat(restart.uniqueKey()).isEqualTo(event(KEY, 41, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                profile(2, "build-a"), 4, INTENT, POSITIONS).uniqueKey());
        assertThatThrownBy(() -> event(KEY, 41, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                PROFILE, 2, INTENT, POSITIONS)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ownerHandoverAtEveryDurableStepDoesNotConsumeAnAttemptOrAllocateAnotherGeneration() {
        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        ClusterRecoveryItem allocated = permitted.advanced(successor(42), NOW);
        ClusterRecoveryItem submitted = allocated.submitted("job-42", NOW);
        ClusterRecoveryItem initialized = submitted.initialized(receipt(submitted, POSITIONS), NOW);

        for (ClusterRecoveryItem step : new ClusterRecoveryItem[] {permitted, allocated, submitted, initialized}) {
            ClusterRecoveryItem adopted = step.permitted(step.permit().handover(recoveryClaim(2)), NOW.plusSeconds(1));
            assertThat(adopted.attempt()).isEqualTo(step.attempt());
            assertThat(adopted.executionAliases()).isEqualTo(step.executionAliases());
            assertThat(adopted.successor()).isEqualTo(step.successor());
            assertThat(adopted.permit().reservationId()).isEqualTo("reservation-1");
            assertThat(adopted.permit().transferredExecutionGeneration())
                    .isEqualTo(step.permit().transferredExecutionGeneration());
            assertThat(adopted.check(fence(step, recoveryClaim(1)), INTENT, PROFILE, step.executionFrontier()))
                    .isEqualTo(ClusterRecoveryMutation.STALE_ITEM);
        }
        assertThat(permitted.attempt()).isZero();
        assertThat(allocated.attempt()).isEqualTo(1);
        assertThat(initialized.recovered(NOW).executionFrontier()).isEqualTo(42);
    }

    @Test
    void permitAdvanceAndSubmitCannotManufactureRecoveryCompletion() {
        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        ClusterRecoveryItem allocated = permitted.advanced(successor(42), NOW);
        ClusterRecoveryItem submitted = allocated.submitted("job-42", NOW);

        for (ClusterRecoveryItem step : new ClusterRecoveryItem[] {queued(), permitted, allocated, submitted}) {
            assertThat(step.completionCheck()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
            assertThatThrownBy(() -> step.recovered(NOW)).isInstanceOf(IllegalStateException.class);
        }
        assertThat(submitted.permit().transferredExecutionGeneration()).isEqualTo(42);
        assertThat(submitted.status()).isEqualTo(ClusterRecoveryStatus.REBUILDING);
        ClusterRecoveryItem initialized = submitted.initialized(receipt(submitted, POSITIONS), NOW);
        assertThat(initialized.completionCheck()).isEqualTo(ClusterRecoveryMutation.APPLIED);
        assertThat(initialized.recovered(NOW).status()).isEqualTo(ClusterRecoveryStatus.RECOVERED);
    }

    @Test
    void startupEvidenceMustMatchTheExactSuccessorAndOriginalResumePosition() {
        ClusterRecoveryItem submitted = allocated().submitted("job-42", NOW);
        ClusterRecoveryStartupReceipt wrongExecution = new ClusterRecoveryStartupReceipt(
                pipelineClaim(43), "job-42", NOW, POSITIONS, NOW, false);
        Map<String, ClusterRecoveryPosition> changed = Map.of("crm", new ClusterRecoveryPosition(
                "crm", "mongo", "capture-orders", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                new ChainPosition(new SourceOrder(7, 94), "resume-94"), "capture-majority-read", "srs/crm/7"));

        assertThat(submitted.startupReceiptCheck(wrongExecution)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        assertThat(submitted.startupReceiptCheck(receipt(submitted, changed)))
                .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        assertThat(submitted.startupReceiptCheck(receipt(submitted, Map.of())))
                .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        assertThat(submitted.startupReceiptCheck(receipt(submitted, POSITIONS)))
                .isEqualTo(ClusterRecoveryMutation.APPLIED);
    }

    @Test
    void poisonedHeadBacksOffThenFailsVisiblyWithoutBlockingLaterRunnableItems() {
        ClusterRecoveryItem poisoned = queued();
        ClusterRecoveryItem later = ClusterRecoveryItem.enqueued(event(new ClusterRecoveryKey("east", "payments", "b"),
                9, ClusterRecoveryCause.MEMBER_LOSS, PROFILE, 2, "payments-intent", POSITIONS), 2, NOW, 3);
        ClusterRecoveryDiagnostic refusal = diagnostic(ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED);
        Duration backoff = Duration.ofSeconds(5);

        poisoned = poisoned.refused(refusal, backoff, NOW);
        assertThat(poisoned.eligibleAt(NOW.plusSeconds(4))).isFalse();
        assertThat(poisoned.eligibleAt(NOW.plusSeconds(5))).isTrue();
        assertThat(later.eligibleAt(NOW)).isTrue();
        poisoned = poisoned.refused(refusal, backoff, NOW.plusSeconds(5));
        poisoned = poisoned.refused(refusal, backoff, NOW.plusSeconds(10));

        assertThat(poisoned.status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
        assertThat(poisoned.attempt()).isEqualTo(3);
        assertThat(poisoned.executionAliases()).containsExactly(41L);
        assertThat(poisoned.successor()).isNull();
        assertThat(poisoned.permit()).isNull();
        assertThat(poisoned.completionCheck()).isEqualTo(ClusterRecoveryMutation.TERMINAL);
        assertThat(poisoned.eligibleAt(NOW.plusSeconds(100))).isFalse();
        assertThat(later.eligibleAt(NOW.plusSeconds(10))).isTrue();
    }

    @Test
    void aFailedAllocatedAttemptIsNotCountedTwiceAndItsGenerationIsNeverReused() {
        ClusterRecoveryItem failed = allocated().executionFailed(
                diagnostic(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED), Duration.ofSeconds(1), NOW);
        assertThat(failed.attempt()).isEqualTo(1);
        assertThat(failed.executionAliases()).containsExactlyInAnyOrder(41L, 42L);
        assertThat(failed.successor().executionGeneration()).isEqualTo(42);
        ClusterRecoveryItem next = failed.permitted(permit("reservation-2", recoveryClaim(2)), NOW.plusSeconds(1))
                .advanced(successor(43), NOW.plusSeconds(1));
        assertThat(next.attempt()).isEqualTo(2);
        assertThat(next.executionAliases()).containsExactlyInAnyOrder(41L, 42L, 43L);
        assertThatThrownBy(() -> failed.permitted(permit("reservation-2", recoveryClaim(2)), NOW)
                .advanced(successor(42), NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourcePositionRejectionIsTerminalAndPreservesTheCodedCauseAndOriginalProvenance() {
        ClusterRecoveryDiagnostic cause = diagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED);
        ClusterRecoveryItem failed = allocated().executionFailed(cause, Duration.ofSeconds(1), NOW);
        assertThat(failed.status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
        assertThat(failed.attempt()).isEqualTo(1);
        assertThat(failed.diagnostic()).isEqualTo(cause);
        assertThat(failed.diagnostic().positions()).isEqualTo(POSITIONS);
        assertThat(failed.diagnostic().code()).isEqualTo(LifecycleError.PIPELINE_NOT_RUNNABLE.code());
        assertThat(failed.diagnostic().params()).containsEntry("pipeline", "orders");
        assertThat(failed.matchesActiveAlias(event())).isFalse();
    }

    @Test
    void persistedDiagnosticsRetainNamespacedConnectorCodes() {
        ClusterRecoveryDiagnostic loaded = new ClusterRecoveryDiagnostic(
                ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                "connector.mongo.position-expired", Map.of("capture", "capture-orders"), POSITIONS,
                "Restore the original retained position before starting explicitly");
        ClusterRecoveryItem failed = allocated().executionFailed(loaded, Duration.ofSeconds(1), NOW);
        assertThat(failed.diagnostic()).isEqualTo(loaded);
        assertThat(failed.status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
    }

    @Test
    void terminalItemsCannotBeReactivatedByAnOldPermitOrEvent() {
        ClusterRecoveryItem submitted = allocated().submitted("job-42", NOW);
        ClusterRecoveryItem recovered = submitted.initialized(receipt(submitted, POSITIONS), NOW).recovered(NOW);
        ClusterRecoveryItem cancelled = queued().cancelled(NOW);
        ClusterRecoveryItem failed = queued().refused(diagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED),
                Duration.ofSeconds(1), NOW);
        for (ClusterRecoveryItem terminal : new ClusterRecoveryItem[] {recovered, cancelled, failed}) {
            assertThat(terminal.check(fence(terminal, recoveryClaim(1)), INTENT, PROFILE,
                    terminal.executionFrontier())).isEqualTo(ClusterRecoveryMutation.TERMINAL);
            assertThat(terminal.matchesActiveAlias(event())).isFalse();
            assertThatThrownBy(() -> terminal.permitted(permit("late", recoveryClaim(2)), NOW))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void snapshotResumeHasExplicitAbsenceAndMustBeAcceptedAsThatSameOriginalMode() {
        ClusterRecoveryPosition snapshot = new ClusterRecoveryPosition("crm", "mongo", "capture-orders",
                ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED, null, "capture-snapshot-incomplete", null);
        Map<String, ClusterRecoveryPosition> snapshotPositions = Map.of("crm", snapshot);
        ClusterRecoveryItem item = ClusterRecoveryItem.enqueued(event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 2, INTENT, snapshotPositions), 1, NOW, 3)
                .permitted(permit("snapshot", recoveryClaim(1)), NOW).advanced(successor(42), NOW)
                .submitted("job-42", NOW);
        assertThat(item.startupReceiptCheck(receipt(item, POSITIONS)))
                .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        assertThat(item.initialized(receipt(item, snapshotPositions), NOW).recovered(NOW).status())
                .isEqualTo(ClusterRecoveryStatus.RECOVERED);
        assertThatThrownBy(() -> new ClusterRecoveryPosition("crm", "mongo", "capture-orders",
                ClusterRecoveryPosition.Kind.DURABLE_POSITION, null, "unknown", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ClusterRecoveryPosition("crm", "mongo", "capture-orders",
                ClusterRecoveryPosition.Kind.DURABLE_POSITION, new ChainPosition(null, null), "unknown", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fingerprintIncludesIncarnationAndEveryLifecycleInstruction() {
        DesiredState intent = new DesiredState("orders", PipelineState.RUNNING, "revision-a", false,
                "assembly-a", false, null);
        String expected = ClusterRecoveryIntentFingerprint.of("a", intent);
        assertThat(ClusterRecoveryIntentFingerprint.of("a", intent)).isEqualTo(expected);
        assertThat(ClusterRecoveryIntentFingerprint.of("b", intent)).isNotEqualTo(expected);
        assertThat(ClusterRecoveryIntentFingerprint.of("a", new DesiredState("orders", PipelineState.RUNNING,
                "revision-a", true, "assembly-a", false, null))).isNotEqualTo(expected);
        assertThat(ClusterRecoveryIntentFingerprint.of("a", new DesiredState("orders", PipelineState.RUNNING,
                "revision-a", false, "assembly-a", true, null))).isNotEqualTo(expected);
        assertThat(ClusterRecoveryIntentFingerprint.of("a", new DesiredState("orders", PipelineState.RUNNING,
                "revision-a", false, "assembly-a", false, 7L))).isNotEqualTo(expected);
        assertThat(ClusterRecoveryIntentFingerprint.of("a", new DesiredState("orders", PipelineState.PAUSED,
                "revision-a", false, "assembly-a", false, null))).isNotEqualTo(expected);
    }

    private static ClusterRecoveryItem queued() {
        return ClusterRecoveryItem.enqueued(event(), 1, NOW, 3);
    }

    private static ClusterRecoveryItem allocated() {
        return queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW).advanced(successor(42), NOW);
    }

    private static ClusterRecoveryEvent event() {
        return event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS, PROFILE, 2, INTENT, POSITIONS);
    }

    private static ClusterRecoveryEvent event(ClusterRecoveryKey key, long generation, ClusterRecoveryCause cause,
            ClusterExecutionProfile target, long topology, String intent, Map<String, ClusterRecoveryPosition> positions) {
        return new ClusterRecoveryEvent(key, cause, generation, "revision-a", 1L, PROFILE, target, topology, intent, positions);
    }

    private static ClusterExecutionProfile profile(long generation, String build) {
        return new ClusterExecutionProfile("east", generation, new ExecutionProfile(1, Map.of("build", build)));
    }

    private static WorkloadClaimFence recoveryClaim(long generation) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CLUSTER_RECOVERY, "east"),
                new WorkloadOwner("node-a", "boot-" + generation), generation, 0, 2, 1);
    }

    private static WorkloadClaimFence pipelineClaim(long executionGeneration) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("node-a", "pipeline-boot"), 7, executionGeneration, 2, 1);
    }

    private static ClusterRecoveryFence fence(ClusterRecoveryItem item, WorkloadClaimFence claim) {
        return new ClusterRecoveryFence(item.event().key(), item.itemRevision(), item.event().intentFingerprint(),
                item.targetProfile(), item.executionFrontier(), claim);
    }

    private static ClusterRecoveryPermit permit(String id, WorkloadClaimFence recoveryClaim) {
        return new ClusterRecoveryPermit(id, recoveryClaim, NOW, NOW.plusSeconds(30), DEMAND, 0);
    }

    private static ClusterRecoverySuccessor successor(long executionGeneration) {
        return new ClusterRecoverySuccessor(pipelineClaim(executionGeneration), PROFILE, Set.of("node-a"), NOW,
                null, null, null);
    }

    private static ClusterRecoveryStartupReceipt receipt(ClusterRecoveryItem item,
            Map<String, ClusterRecoveryPosition> positions) {
        return new ClusterRecoveryStartupReceipt(item.successor().pipelineClaim(), item.successor().nativeJobId(),
                NOW, positions, NOW, false);
    }

    private static ClusterRecoveryDiagnostic diagnostic(ClusterRecoveryDiagnostic.Reason reason) {
        return ClusterRecoveryDiagnostic.from(reason, new TapstateException(LifecycleError.PIPELINE_NOT_RUNNABLE,
                Map.of("pipeline", "orders"), null), POSITIONS, "Correct the source configuration and start explicitly");
    }

    private static String fingerprint(PipelineState state) {
        return ClusterRecoveryIntentFingerprint.of(KEY.incarnation(), new DesiredState(KEY.pipelineId(), state, "revision-a"));
    }
}
