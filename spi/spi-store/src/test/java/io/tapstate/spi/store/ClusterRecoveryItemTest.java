package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.ReadMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
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
    void startupEvidenceMustMatchTheExactSuccessorAndItsPreparedRequest() {
        ClusterRecoveryItem submitted = allocated().submitted("job-42", NOW);
        ClusterRecoveryStartupReceipt wrongExecution = new ClusterRecoveryStartupReceipt(
                pipelineClaim(43), "job-42", NOW, POSITIONS, NOW, false);
        Map<String, ClusterRecoveryPosition> changed = Map.of("crm", new ClusterRecoveryPosition(
                "crm", "mongo", "capture-orders", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                new ChainPosition(new SourceOrder(7, 94), "resume-94"), "capture-majority-read", "srs/crm/7"));

        assertThat(submitted.startupReceiptCheck(wrongExecution)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        ClusterRecoveryStartupReceipt incorrect = new ClusterRecoveryStartupReceipt(submitted.successor().pipelineClaim(),
                "job-42", NOW, Map.of("crm", witness()), POSITIONS, changed, NOW, false);
        assertThat(submitted.startupReceiptCheck(incorrect))
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
    void snapshotResumeHasExplicitAbsenceAndMustBeAcceptedAsTheActualPreparedMode() {
        ClusterRecoveryPosition snapshot = new ClusterRecoveryPosition("crm", "mongo", "capture-orders",
                ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED, null, "capture-snapshot-incomplete", null);
        Map<String, ClusterRecoveryPosition> snapshotPositions = Map.of("crm", snapshot);
        ClusterRecoveryItem item = ClusterRecoveryItem.enqueued(event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 2, INTENT, snapshotPositions), 1, NOW, 3)
                .permitted(permit("snapshot", recoveryClaim(1)), NOW).advanced(successor(42), NOW)
                .submitted("job-42", NOW);
        ClusterRecoveryStartupReceipt incorrect = new ClusterRecoveryStartupReceipt(item.successor().pipelineClaim(),
                "job-42", NOW, Map.of("crm", witness()), snapshotPositions, POSITIONS, NOW, false);
        assertThat(item.startupReceiptCheck(incorrect))
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
    void fingerprintIncludesEveryLifecycleInstructionWhileIncarnationIsIndependentlyFenced() {
        DesiredState intent = new DesiredState("orders", PipelineState.RUNNING, "revision-a", false,
                "assembly-a", false, null);
        String expected = DesiredStateFingerprint.of(intent);
        assertThat(DesiredStateFingerprint.of(intent)).isEqualTo(expected);
        assertThat(DesiredStateFingerprint.of(new DesiredState("orders", PipelineState.RUNNING,
                "revision-a", true, "assembly-a", false, null))).isNotEqualTo(expected);
        assertThat(DesiredStateFingerprint.of(new DesiredState("orders", PipelineState.RUNNING,
                "revision-a", false, "assembly-a", true, null))).isNotEqualTo(expected);
        assertThat(DesiredStateFingerprint.of(new DesiredState("orders", PipelineState.RUNNING,
                "revision-a", false, "assembly-a", false, 7L))).isNotEqualTo(expected);
        assertThat(DesiredStateFingerprint.of(new DesiredState("orders", PipelineState.PAUSED,
                "revision-a", false, "assembly-a", false, null))).isNotEqualTo(expected);
    }

    @Test
    void aPersistedItemWithAnyInconsistentIdentityRevisionOrStepIsRefused() {
        assertThat(new Raw().build().status()).isEqualTo(ClusterRecoveryStatus.WAITING_PERMIT);
        Map<String, Consumer<Raw>> invalid = Map.ofEntries(
                change("unknown schema", raw -> raw.schema = ClusterRecoveryItem.SCHEMA_VERSION + 1),
                change("unwritten revision", raw -> raw.revision = 0),
                change("unassigned sequence", raw -> raw.sequence = 0),
                change("no target topology", raw -> raw.topology = 0),
                change("negative attempt", raw -> raw.attempt = -1),
                change("no retry budget", raw -> raw.max = 0),
                change("attempt beyond budget", raw -> raw.attempt = 4),
                change("target behind the event", raw -> raw.event = event(KEY, 41,
                        ClusterRecoveryCause.FULL_CLUSTER_RESTART, profile(2, "build-a"), 3, INTENT, POSITIONS)),
                change("target in another cluster", raw -> raw.target = westProfile()),
                change("original execution forgotten", raw -> raw.aliases = Set.of(42L)),
                change("non-positive alias", raw -> raw.aliases = Set.of(41L, 0L)),
                change("more aliases than attempts", raw -> raw.aliases = Set.of(41L, 42L, 43L, 44L, 45L)),
                change("updated before enqueued", raw -> raw.updated = NOW.minusSeconds(1)),
                change("terminal item keeps a permit", raw -> {
                    raw.status = ClusterRecoveryStatus.CANCELLED;
                    raw.permit = permit("reservation-1", recoveryClaim(1));
                }),
                change("backoff without eligibility", raw -> {
                    raw.status = ClusterRecoveryStatus.RETRY_BACKOFF;
                    raw.next = null;
                }),
                change("rebuilding without permit", raw -> raw.status = ClusterRecoveryStatus.REBUILDING),
                change("failed without diagnostic", raw -> raw.status = ClusterRecoveryStatus.REBUILD_FAILED),
                change("waiting item holds a permit", raw -> raw.permit = permit("reservation-1", recoveryClaim(1))));
        invalid.forEach((name, change) -> assertThatThrownBy(() -> new Raw().with(change).build())
                .as(name).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("recovery item identity, revision or step is invalid"));
    }

    @Test
    void aSuccessorOrTransferredReservationOutsideThisItemsExecutionsIsRefused() {
        assertThat(rebuilding().build().successor().executionGeneration()).isEqualTo(42);
        WorkloadClaimFence otherPipeline = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "payments"),
                new WorkloadOwner("node-a", "pipeline-boot"), 7, 42, 2, 1);
        WorkloadClaimFence otherCluster = new WorkloadClaimFence(
                new WorkloadClaimKey("west", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("node-a", "pipeline-boot"), 7, 42, 2, 1);
        Map<String, Consumer<Raw>> outside = Map.of(
                "successor outside the aliases", raw -> raw.aliases = Set.of(41L, 43L),
                "successor of another pipeline", raw -> raw.successor = successor(otherPipeline, PROFILE),
                "successor in another cluster", raw -> raw.successor = successor(otherCluster, westProfile()),
                "successor not after the original", raw -> raw.successor = successor(41),
                "successor before any attempt", raw -> raw.attempt = 0);
        outside.forEach((name, change) -> assertThatThrownBy(() -> rebuilding().with(change).build())
                .as(name).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("successor must belong to this item's execution aliases"));

        Map<String, Consumer<Raw>> transferred = Map.of(
                "transferred without a successor", raw -> {
                    raw.successor = null;
                    raw.permit = permit("reservation-1", recoveryClaim(1)).transferred(42);
                },
                "transferred to another execution", raw -> {
                    raw.aliases = Set.of(41L, 42L, 43L);
                    raw.permit = permit("reservation-1", recoveryClaim(1)).transferred(43);
                });
        transferred.forEach((name, change) -> assertThatThrownBy(() -> rebuilding().with(change).build())
                .as(name).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("transferred reservation must match the successor"));
    }

    @Test
    void aRecoveredItemRequiresTheInitializedSuccessorOfItsTargetProfileAndTopology() {
        ClusterRecoveryItem submitted = allocated().submitted("job-42", NOW);
        ClusterRecoverySuccessor initialized = submitted.initialized(receipt(submitted, POSITIONS), NOW).successor();
        assertThat(recovered(initialized).build().status()).isEqualTo(ClusterRecoveryStatus.RECOVERED);

        Map<String, Consumer<Raw>> unproven = Map.of(
                "no startup receipt", raw -> raw.successor = successor(42),
                "successor of an older profile", raw -> raw.target = profile(2, "build-a"),
                "target topology behind the successor", raw -> raw.topology = 1);
        unproven.forEach((name, change) -> assertThatThrownBy(() -> recovered(initialized).with(change).build())
                .as(name).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("recovered item requires matching initialization and source acceptance"));
    }

    @Test
    void aTerminalItemIsReplacedOnlyByANewerLossOfItsLatestExecution() {
        ClusterRecoveryItem submitted = allocated().submitted("job-42", NOW);
        ClusterRecoveryItem recovered = submitted.initialized(receipt(submitted, POSITIONS), NOW).recovered(NOW);
        ClusterRecoveryItem cancelled = queued().cancelled(NOW);

        assertThatThrownBy(() -> queued().terminalEventCheck(event())).isInstanceOf(IllegalStateException.class);
        assertThat(recovered.terminalEventCheck(event(new ClusterRecoveryKey("east", "orders", "incarnation-b"),
                42, ClusterRecoveryCause.MEMBER_LOSS, PROFILE, 3, INTENT, POSITIONS)))
                .isEqualTo(ClusterRecoveryMutation.STALE_ITEM);
        assertThat(recovered.terminalEventCheck(event())).isEqualTo(ClusterRecoveryMutation.TERMINAL);
        assertThat(recovered.terminalEventCheck(event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 3, INTENT, POSITIONS))).isEqualTo(ClusterRecoveryMutation.TERMINAL);
        assertThat(recovered.terminalEventCheck(event(KEY, 42, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 3, INTENT, POSITIONS))).isEqualTo(ClusterRecoveryMutation.APPLIED);
        assertThat(cancelled.terminalEventCheck(event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 3, INTENT, POSITIONS))).isEqualTo(ClusterRecoveryMutation.TERMINAL);
        assertThat(cancelled.terminalEventCheck(event(KEY, 41, ClusterRecoveryCause.MEMBER_LOSS,
                PROFILE, 3, fingerprint(PipelineState.PAUSED), POSITIONS))).isEqualTo(ClusterRecoveryMutation.APPLIED);
    }

    @Test
    void deferralAndRetargetingOnlyMoveForward() {
        ClusterRecoveryItem deferred = queued().deferredUntil(NOW.plusSeconds(10), NOW);
        assertThat(deferred.nextEligibleAt()).isEqualTo(NOW.plusSeconds(10));
        assertThat(deferred.attempt()).isZero();
        assertThat(deferred.eligibleAt(NOW.plusSeconds(9))).isFalse();
        assertThat(deferred.deferredUntil(NOW.plusSeconds(5), NOW.plusSeconds(1)).nextEligibleAt())
                .isEqualTo(NOW.plusSeconds(10));
        assertThat(deferred.deferredUntil(NOW.plusSeconds(20), NOW.plusSeconds(1)).nextEligibleAt())
                .isEqualTo(NOW.plusSeconds(20));
        assertThatThrownBy(() -> queued().deferredUntil(NOW, NOW)).isInstanceOf(IllegalArgumentException.class);
        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        assertThat(permitted.eligibleAt(NOW.plusSeconds(60))).isFalse();
        assertThatThrownBy(() -> permitted.deferredUntil(NOW.plusSeconds(10), NOW))
                .isInstanceOf(IllegalArgumentException.class);

        ClusterRecoveryItem retargeted = queued().retargeted(profile(2, "build-a"), 3, NOW);
        assertThat(retargeted.targetProfile()).isEqualTo(profile(2, "build-a"));
        assertThat(retargeted.targetTopologyRevision()).isEqualTo(3);
        assertThatThrownBy(() -> retargeted.retargeted(PROFILE, 3, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> retargeted.retargeted(profile(2, "build-a"), 2, NOW))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void adoptingAPermitMustKeepItsClusterReservationDemandAndTransfer() {
        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        WorkloadClaimFence westClaim = new WorkloadClaimFence(
                new WorkloadClaimKey("west", WorkloadClaimType.CLUSTER_RECOVERY, "west"),
                new WorkloadOwner("node-a", "boot-1"), 1, 0, 2, 1);
        assertThat(permitted.permitted(permit("reservation-1", recoveryClaim(2)), NOW).permit().recoveryClaim())
                .isEqualTo(recoveryClaim(2));
        List<ClusterRecoveryPermit> changed = List.of(
                permit("reservation-2", recoveryClaim(2)),
                new ClusterRecoveryPermit("reservation-1", recoveryClaim(2), NOW, NOW.plusSeconds(30),
                        Map.of("node-b", new ClusterCapacityDemand(3, 1, 1, 2, 200, 400)), 0),
                new ClusterRecoveryPermit("reservation-1", recoveryClaim(2), NOW.plusSeconds(1), NOW.plusSeconds(30),
                        DEMAND, 0),
                permit("reservation-1", recoveryClaim(2)).transferred(42));
        assertThat(changed).hasSize(4);
        for (ClusterRecoveryPermit adopted : changed) {
            assertThatThrownBy(() -> permitted.permitted(adopted, NOW)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("permit adoption must retain the reservation and execution");
        }
        assertThatThrownBy(() -> queued().permitted(new ClusterRecoveryPermit("reservation-1", westClaim, NOW,
                NOW.plusSeconds(30), DEMAND, 0), NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anExecutionAdvanceMustConsumeTheExactUntransferredPermit() {
        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        ClusterRecoveryFailureNote note = new ClusterRecoveryFailureNote(pipelineClaim(42),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION,
                diagnostic(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED), NOW);
        WorkloadClaimFence newerProfileClaim = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("node-a", "pipeline-boot"), 7, 42, 2, 2);
        ClusterRecoveryItem exhausted = new Raw().with(raw -> {
            raw.status = ClusterRecoveryStatus.REBUILDING;
            raw.permit = permit("reservation-1", recoveryClaim(1));
            raw.attempt = 3;
        }).build();

        Map<String, ThrowingCallable> refused = Map.of(
                "no permit", () -> queued().advanced(successor(42), NOW),
                "permit already transferred", () -> allocated().advanced(successor(43), NOW),
                "skipped generation", () -> permitted.advanced(successor(43), NOW),
                "already failed successor", () -> permitted.advanced(new ClusterRecoverySuccessor(pipelineClaim(42),
                        PROFILE, Set.of("node-a"), Set.of("crm"), true, NOW, null, null, Map.of(), null, note), NOW),
                "successor of another profile", () -> permitted.advanced(successor(newerProfileClaim,
                        profile(2, "build-a")), NOW),
                "successor on unreserved members", () -> permitted.advanced(new ClusterRecoverySuccessor(
                        pipelineClaim(42), PROFILE, Set.of("node-b"), Set.of("crm"), NOW, null, null, Map.of(), null), NOW),
                "retry budget spent", () -> exhausted.advanced(successor(42), NOW));
        refused.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("execution advance must consume the exact untransferred permit"));
    }

    @Test
    void anAllocationProofBindsThePermitOnlyToTheRecordedSuccessor() {
        ClusterRecoveryItem recorded = rebuilding().build();
        assertThat(recorded.hasAllocatedSuccessor()).isFalse();
        ClusterRecoveryItem proven = recorded.allocationRecorded(42, NOW.plusSeconds(1));
        assertThat(proven.permit().transferredExecutionGeneration()).isEqualTo(42);
        assertThat(proven.hasAllocatedSuccessor()).isTrue();
        assertThat(proven.itemRevision()).isEqualTo(recorded.itemRevision() + 1);

        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        Map<String, ThrowingCallable> refused = Map.of(
                "another generation", () -> recorded.allocationRecorded(43, NOW),
                "no successor", () -> permitted.allocationRecorded(41, NOW),
                "already transferred", () -> proven.allocationRecorded(42, NOW),
                "no permit", () -> queued().allocationRecorded(41, NOW));
        refused.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("allocation proof must bind this permit to the recorded successor"));
    }

    @Test
    void submissionStartupAndFailureFactsMustMatchTheAllocatedSuccessor() {
        ClusterRecoveryItem permitted = queued().permitted(permit("reservation-1", recoveryClaim(1)), NOW);
        ClusterRecoveryItem submitted = allocated().submitted("job-42", NOW);
        assertThatThrownBy(() -> submitted.submitted("job-43", NOW)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> permitted.submitted("job-42", NOW)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> submitted.initialized(receipt(submitted, Map.of()), NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> allocated().initialized(receipt(submitted, POSITIONS), NOW))
                .isInstanceOf(IllegalArgumentException.class);

        ClusterRecoveryDiagnostic first = diagnostic(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED);
        ClusterRecoveryFailureNote note = new ClusterRecoveryFailureNote(pipelineClaim(42),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, first, NOW);
        assertThatThrownBy(() -> permitted.failureNoted(note, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> submitted.failureNoted(new ClusterRecoveryFailureNote(pipelineClaim(43),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, first, NOW), NOW))
                .isInstanceOf(IllegalArgumentException.class);
        ClusterRecoveryItem noted = submitted.failureNoted(note, NOW.plusSeconds(1));
        assertThat(noted.successor().failureNote()).isEqualTo(note);
        assertThat(noted.diagnostic()).isEqualTo(first);
        assertThat(noted.permit()).isEqualTo(submitted.permit());
        assertThat(noted.failureNoted(new ClusterRecoveryFailureNote(pipelineClaim(42),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION,
                diagnostic(ClusterRecoveryDiagnostic.Reason.REBUILD_REFUSED), NOW), NOW.plusSeconds(2)))
                .isSameAs(noted);
        assertThatThrownBy(() -> noted.submitted("job-44", NOW)).isInstanceOf(IllegalStateException.class);

        ClusterRecoveryItem failed = noted.executionFailed(
                diagnostic(ClusterRecoveryDiagnostic.Reason.REBUILD_REFUSED), Duration.ofSeconds(1), NOW.plusSeconds(3));
        assertThat(failed.diagnostic()).as("the first noted cause outlives a later report").isEqualTo(first);
        assertThat(failed.status()).isEqualTo(ClusterRecoveryStatus.RETRY_BACKOFF);
    }

    @Test
    void retriesNeedARemainingBudgetAnAllocatedSuccessorAndAPositiveBackoff() {
        ClusterRecoveryDiagnostic refusal = diagnostic(ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED);
        ClusterRecoveryItem spent = new Raw().with(raw -> {
            raw.status = ClusterRecoveryStatus.RETRY_BACKOFF;
            raw.attempt = 3;
            raw.diagnostic = refusal;
        }).build();
        assertThatThrownBy(() -> spent.refused(refusal, Duration.ofSeconds(1), NOW))
                .isInstanceOf(IllegalStateException.class).hasMessage("the retry budget is exhausted");
        assertThatThrownBy(() -> queued().executionFailed(refusal, Duration.ofSeconds(1), NOW))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> queued().refused(refusal, Duration.ZERO, NOW))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("retry backoff must be positive");
        assertThatThrownBy(() -> queued().refused(refusal, Duration.ofSeconds(-1), NOW))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("retry backoff must be positive");
        assertThatThrownBy(() -> queued().cancelled(NOW).cancelled(NOW)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aSuccessorMustNameOneSubmittedPipelineExecutionOfItsProfile() {
        WorkloadClaimFence claim = pipelineClaim(42);
        assertThat(successor(claim, PROFILE).executionGeneration()).isEqualTo(42);
        WorkloadClaimFence recoveryTyped = recoveryClaim(1);
        WorkloadClaimFence unallocated = new WorkloadClaimFence(claim.key(), claim.owner(), 7, 0, 2, 1);
        Map<String, ThrowingCallable> invalidIdentity = Map.of(
                "recovery claim", () -> successor(recoveryTyped, PROFILE),
                "no execution", () -> successor(unallocated, PROFILE),
                "no members", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of(), Set.of("crm"), NOW,
                        null, null, Map.of(), null),
                "blank member", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of(" "), Set.of("crm"), NOW,
                        null, null, Map.of(), null),
                "blank source", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"), Set.of(""), NOW,
                        null, null, Map.of(), null),
                "unrecorded sources named", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                        Set.of("crm"), false, NOW, null, null, Map.of(), null),
                "profile of another cluster", () -> successor(claim, westProfile()),
                "profile of another generation", () -> successor(claim, profile(2, "build-a")),
                "job without submission time", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                        Set.of("crm"), NOW, "job-42", null, Map.of(), null),
                "submission time without job", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                        Set.of("crm"), NOW, null, NOW, Map.of(), null));
        invalidIdentity.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("recovery successor identity or submission fields are invalid"));

        ClusterRecoveryStartupReceipt started = new ClusterRecoveryStartupReceipt(claim, "job-42", NOW,
                Map.of("crm", witness()), POSITIONS, POSITIONS, NOW, false);
        Map<String, ThrowingCallable> mismatchedReceipt = Map.of(
                "receipt before submission", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                        Set.of("crm"), NOW, null, null, Map.of(), started),
                "receipt of another execution", () -> new ClusterRecoverySuccessor(pipelineClaim(43), PROFILE,
                        Set.of("node-a"), Set.of("crm"), NOW, "job-42", NOW, Map.of(), started),
                "receipt of another job", () -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                        Set.of("crm"), NOW, "job-41", NOW, Map.of(), started));
        mismatchedReceipt.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("startup receipt must match the submitted successor"));
        assertThatThrownBy(() -> new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"), Set.of("crm"), true,
                NOW, null, null, Map.of(), null, new ClusterRecoveryFailureNote(pipelineClaim(43),
                        ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION,
                        diagnostic(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED), NOW)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("failure note must match the allocated successor");
    }

    @Test
    void startupMatchesOnlyAReceiptThatAcceptedExactlyTheRequiredSources() {
        WorkloadClaimFence claim = pipelineClaim(42);
        ClusterRecoverySuccessor submitted = new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                Set.of("crm"), NOW, "job-42", NOW, POSITIONS, null);
        ClusterRecoveryStartupReceipt exact = new ClusterRecoveryStartupReceipt(claim, "job-42", NOW,
                Map.of("crm", witness()), POSITIONS, POSITIONS, NOW, false);
        assertThat(submitted.matchesStartup(exact)).isTrue();

        ClusterRecoveryPosition moved = new ClusterRecoveryPosition("crm", "mongo", "capture-orders",
                ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                new ChainPosition(new SourceOrder(7, 94), "resume-94"), "capture-majority-read", "srs/crm/7");
        ClusterRecoverySuccessor unrecorded = new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                Set.of(), false, NOW, "job-42", NOW, Map.of(), null, null);
        ClusterRecoverySuccessor failed = new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                Set.of("crm"), true, NOW, "job-42", NOW, POSITIONS, null, new ClusterRecoveryFailureNote(claim,
                        ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION,
                        diagnostic(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED), NOW));
        ClusterRecoverySuccessor otherRequest = new ClusterRecoverySuccessor(claim, PROFILE, Set.of("node-a"),
                Set.of("crm"), NOW, "job-42", NOW, Map.of("crm", moved), null);
        Map<String, Boolean> matches = new java.util.LinkedHashMap<>();
        matches.put("no receipt", submitted.matchesStartup(null));
        matches.put("failed successor", failed.matchesStartup(exact));
        matches.put("source requirements never recorded", unrecorded.matchesStartup(
                new ClusterRecoveryStartupReceipt(claim, "job-42", NOW, Map.of(), Map.of(), Map.of(), NOW, false)));
        matches.put("another execution", submitted.matchesStartup(new ClusterRecoveryStartupReceipt(pipelineClaim(43),
                "job-42", NOW, Map.of("crm", witness()), POSITIONS, POSITIONS, NOW, false)));
        matches.put("another job", submitted.matchesStartup(new ClusterRecoveryStartupReceipt(claim, "job-41", NOW,
                Map.of("crm", witness()), POSITIONS, POSITIONS, NOW, false)));
        matches.put("no prepared witness", submitted.matchesStartup(new ClusterRecoveryStartupReceipt(claim, "job-42",
                NOW, Map.of(), POSITIONS, POSITIONS, NOW, false)));
        matches.put("nothing requested", submitted.matchesStartup(new ClusterRecoveryStartupReceipt(claim, "job-42",
                NOW, Map.of("crm", witness()), Map.of(), POSITIONS, NOW, false)));
        matches.put("nothing accepted", submitted.matchesStartup(new ClusterRecoveryStartupReceipt(claim, "job-42",
                NOW, Map.of("crm", witness()), POSITIONS, Map.of(), NOW, false)));
        matches.put("accepted another position", submitted.matchesStartup(new ClusterRecoveryStartupReceipt(claim,
                "job-42", NOW, Map.of("crm", witness()), POSITIONS, Map.of("crm", moved), NOW, false)));
        matches.put("requested another position", otherRequest.matchesStartup(exact));
        assertThat(matches).hasSize(10).containsOnly(
                Map.entry("no receipt", false), Map.entry("failed successor", false),
                Map.entry("source requirements never recorded", false), Map.entry("another execution", false),
                Map.entry("another job", false), Map.entry("no prepared witness", false),
                Map.entry("nothing requested", false), Map.entry("nothing accepted", false),
                Map.entry("accepted another position", false), Map.entry("requested another position", false));
    }

    /** A persisted item's fields, valid as a fresh queued step until a case changes one. */
    private static final class Raw {
        int schema = ClusterRecoveryItem.SCHEMA_VERSION;
        ClusterRecoveryEvent event = event();
        long revision = 1;
        long sequence = 1;
        Instant enqueued = NOW;
        Instant updated = NOW;
        ClusterExecutionProfile target = PROFILE;
        long topology = 2;
        ClusterRecoveryStatus status = ClusterRecoveryStatus.WAITING_PERMIT;
        int attempt;
        int max = 3;
        Instant next = NOW;
        Set<Long> aliases = Set.of(41L);
        ClusterRecoveryPermit permit;
        ClusterRecoverySuccessor successor;
        ClusterRecoveryDiagnostic diagnostic;

        Raw with(Consumer<Raw> change) {
            change.accept(this);
            return this;
        }

        ClusterRecoveryItem build() {
            return new ClusterRecoveryItem(schema, event, revision, sequence, enqueued, updated, target, topology,
                    status, attempt, max, next, aliases, permit, successor, diagnostic);
        }
    }

    /** An allocated attempt whose successor is recorded but whose reservation has not yet been transferred. */
    private static Raw rebuilding() {
        return new Raw().with(raw -> {
            raw.status = ClusterRecoveryStatus.REBUILDING;
            raw.permit = permit("reservation-1", recoveryClaim(1));
            raw.attempt = 1;
            raw.next = null;
            raw.aliases = Set.of(41L, 42L);
            raw.successor = successor(42);
        });
    }

    private static Raw recovered(ClusterRecoverySuccessor initialized) {
        return new Raw().with(raw -> {
            raw.status = ClusterRecoveryStatus.RECOVERED;
            raw.attempt = 1;
            raw.next = null;
            raw.aliases = Set.of(41L, 42L);
            raw.successor = initialized;
        });
    }

    private static Map.Entry<String, Consumer<Raw>> change(String name, Consumer<Raw> change) {
        return Map.entry(name, change);
    }

    private static ClusterExecutionProfile westProfile() {
        return new ClusterExecutionProfile("west", 1, new ExecutionProfile(1, Map.of("build", "build-a")));
    }

    private static ClusterRecoverySuccessor successor(WorkloadClaimFence claim, ClusterExecutionProfile profile) {
        return new ClusterRecoverySuccessor(claim, profile, Set.of("node-a"), Set.of("crm"), NOW, null, null, Map.of(), null);
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
        return new ClusterRecoverySuccessor(pipelineClaim(executionGeneration), PROFILE, Set.of("node-a"), Set.of("crm"), NOW,
                null, null, Map.of(), null);
    }

    private static ClusterRecoveryStartupReceipt receipt(ClusterRecoveryItem item,
            Map<String, ClusterRecoveryPosition> positions) {
        return new ClusterRecoveryStartupReceipt(item.successor().pipelineClaim(), item.successor().nativeJobId(),
                NOW, Map.of("crm", witness()), positions, positions, NOW, false);
    }

    private static CaptureResumeWitness witness() {
        ChainPosition floor = POSITIONS.get("crm").position();
        return new CaptureResumeWitness("crm", "mongo", "capture-orders", SrsConsumerId.of("orders", "crm").value(),
                ReadMode.CDC_ONLY, true, List.of("orders"), true, 7, floor, true, true, List.of(), null, 0,
                ConsumerProgressKind.SRS, floor, Map.of("orders", floor));
    }

    private static ClusterRecoveryDiagnostic diagnostic(ClusterRecoveryDiagnostic.Reason reason) {
        return ClusterRecoveryDiagnostic.from(reason, new TapstateException(LifecycleError.PIPELINE_NOT_RUNNABLE,
                Map.of("pipeline", "orders"), null), POSITIONS, "Correct the source configuration and start explicitly");
    }

    private static String fingerprint(PipelineState state) {
        return DesiredStateFingerprint.of(new DesiredState(KEY.pipelineId(), state, "revision-a"));
    }
}
