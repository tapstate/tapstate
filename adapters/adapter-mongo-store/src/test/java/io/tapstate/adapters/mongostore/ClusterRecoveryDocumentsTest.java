package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryDiagnostic;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryPermit;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStartupReceipt;
import io.tapstate.spi.store.ClusterRecoveryMutation;
import io.tapstate.spi.store.ClusterRecoveryFailureNote;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.ClusterRecoverySuccessor;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterRecoveryDocumentsTest {
    private static final Instant NOW = Instant.parse("2026-10-10T01:00:00.123Z");
    private static final ClusterRecoveryKey KEY = new ClusterRecoveryKey("east", "orders", "inc-a");
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("east", 1,
            new ExecutionProfile(1, Map.of("build", "one", "threads", "4")));
    private static final WorkloadClaimFence RECOVERY = fence(WorkloadClaimType.CLUSTER_RECOVERY, "east", 0, 2);
    private static final WorkloadClaimFence PIPELINE = fence(WorkloadClaimType.PIPELINE_ACTUATION, "orders", 42, 2);
    private static final Map<String, ClusterCapacityDemand> DEMAND = Map.of("node.alpha",
            new ClusterCapacityDemand(2, 1, 1, 2, 300, 500));
    private static final Map<String, ClusterRecoveryPosition> POSITIONS = Map.of("crm",
            new ClusterRecoveryPosition("crm", "mongo", "capture-a", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                    new ChainPosition(new SourceOrder(9, 73), "resume:73"), "srs-majority-read", "source-read/crm"));

    @Test
    void everyDurableStepRoundTripsThroughBsonWithoutLosingAliasOrAuthority() {
        ClusterRecoveryItem queued = queued(POSITIONS);
        ClusterRecoveryItem permitted = queued.permitted(new ClusterRecoveryPermit("permit-a", RECOVERY, NOW,
                NOW.plusSeconds(30), DEMAND, 0), NOW);
        ClusterRecoveryItem advanced = permitted.advanced(new ClusterRecoverySuccessor(PIPELINE, PROFILE,
                Set.of("node.alpha"), Set.of("crm"), NOW, null, null, Map.of(), null), NOW);
        ClusterRecoveryItem submitted = advanced.submitted("job-42", NOW);
        ClusterRecoveryItem initialized = submitted.initialized(new ClusterRecoveryStartupReceipt(PIPELINE,
                "job-42", NOW, Map.of("crm", witness()), POSITIONS, POSITIONS, NOW, false), NOW);
        for (ClusterRecoveryItem item : new ClusterRecoveryItem[] {queued, permitted, advanced, submitted,
                initialized, initialized.recovered(NOW)}) {
            Document bson = Document.parse(ClusterRecoveryDocuments.item(item).toJson());
            assertThat(ClusterRecoveryDocuments.item(bson)).isEqualTo(item);
        }
    }

    @Test
    void snapshotOnlyAbsenceAndLegacyColdProfileAbsenceRemainExplicit() {
        Map<String, ClusterRecoveryPosition> snapshot = Map.of("crm", new ClusterRecoveryPosition("crm", "mongo", "capture-a",
                ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED, null, "snapshot-incomplete", null));
        ClusterRecoveryEvent legacy = new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                41, "artifact-revision", null, null, true, PROFILE, 2, "intent", snapshot);
        ClusterRecoveryItem item = ClusterRecoveryItem.enqueued(legacy, 1, NOW, 3);
        Document bson = Document.parse(ClusterRecoveryDocuments.item(item).toJson());
        assertThat(ClusterRecoveryDocuments.item(bson)).isEqualTo(item);
        assertThat(bson.get("event", Document.class).get("sourceProfile")).isNull();
        assertThat(bson.get("event", Document.class).get("sourceTopologyRevision")).isNull();
        assertThat(ClusterRecoveryDocuments.item(bson).event().resumePositions().get("crm").position()).isNull();
    }

    @Test
    void terminalDiagnosticPreservesOriginalNamespacedCodeAndNamedArguments() {
        ClusterRecoveryDiagnostic loaded = new ClusterRecoveryDiagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                "connector.mongo.position-expired", Map.of("capture", "capture-a", "position", "resume:73"), POSITIONS,
                "Restore the retained source position before starting explicitly");
        ClusterRecoveryItem failed = queued(POSITIONS).refused(loaded, Duration.ofSeconds(1), NOW);
        assertThat(ClusterRecoveryDocuments.item(Document.parse(ClusterRecoveryDocuments.item(failed).toJson()))).isEqualTo(failed);
    }

    @Test
    void corruptVersionProfileHashAndIncarnationAreCodedStoreCorruption() {
        Document version = ClusterRecoveryDocuments.item(queued(POSITIONS));
        version.put("schemaVersion", 2);
        assertUnreadable(version);
        Document profile = ClusterRecoveryDocuments.item(queued(POSITIONS));
        profile.get("targetProfile", Document.class).put("hash", "forged");
        assertUnreadable(profile);
        Document identity = ClusterRecoveryDocuments.item(queued(POSITIONS));
        identity.put("incarnation", "recreated");
        assertUnreadable(identity);
    }

    @Test
    void capacityBeforeAdvanceHasNoExecutionNumberAndSubmissionKeepsTheSameReservation() {
        ClusterCapacityReservation reserved = new ClusterCapacityReservation("r", "east", "orders", "inc-a", "intent",
                PROFILE, fence(WorkloadClaimType.PIPELINE_ACTUATION, "orders", 41, 2), DEMAND, NOW, NOW.plusSeconds(30), null, null);
        Document bson = Document.parse(MongoClusterCapacityStore.document(reserved).toJson());
        assertThat(bson.get("executionGeneration")).isNull();
        assertThat(MongoClusterCapacityStore.reservation(bson)).isEqualTo(reserved);
        ClusterCapacityReservation submitted = new ClusterCapacityReservation("r", "east", "orders", "inc-a", "intent",
                PROFILE, PIPELINE, DEMAND, NOW, NOW.plusSeconds(30), 42L, "job-42");
        assertThat(MongoClusterCapacityStore.reservation(Document.parse(MongoClusterCapacityStore.document(submitted).toJson())))
                .isEqualTo(submitted);
    }

    @Test
    void desiredConditionIncludesAllInstructionsAndAbsentLegacyFields() {
        DesiredState desired = new DesiredState("orders", PipelineState.RUNNING, "r", true, "assembly", true, 8L);
        Document stored = MongoDesiredStore.toDocument(desired);
        Document filter = MongoClusterCapacityStore.intentFilter(stored);
        assertThat(filter).containsAllEntriesOf(stored);
        Document legacy = new Document("_id", "orders").append("targetState", "RUNNING").append("revision", "r");
        Document legacyFilter = MongoClusterCapacityStore.intentFilter(legacy);
        assertThat(legacyFilter.get("reassemble")).isEqualTo(new Document("$exists", false));
        assertThat(legacyFilter.get("purgeState")).isEqualTo(new Document("$exists", false));
        assertThat(DesiredStateFingerprint.of(MongoDesiredStore.toDesired(legacy)))
                .isEqualTo(DesiredStateFingerprint.of(new DesiredState("orders", PipelineState.RUNNING, "r")));
    }

    @Test
    void runtimeAndActualStoreUseOneCompleteIntentFingerprint() {
        DesiredState desired = new DesiredState("orders", PipelineState.RUNNING, "revision", true, "assembly", true, 8L);
        assertThat(MongoClusterCapacityStore.intentFingerprint(KEY, MongoDesiredStore.toDocument(desired)))
                .isEqualTo(DesiredStateFingerprint.of(desired));
    }

    @Test
    void legacyExecutionMustHaveItsRealMigratedIncarnationAndCannotBorrowTheTargetRevision() {
        ClusterRecoveryEvent legacy = new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                41, null, null, null, true, PROFILE, 2, "intent", POSITIONS);
        Document source = new Document("executionGeneration", 41L);
        assertThat(MongoClusterRecoveryStore.originalExecutionCheck(legacy, source)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        source.append("executionIncarnation", "inc-a");
        assertThat(MongoClusterRecoveryStore.originalExecutionCheck(legacy, source)).isEqualTo(ClusterRecoveryMutation.APPLIED);
        source.append("executionIncarnation", "recreated");
        assertThat(MongoClusterRecoveryStore.originalExecutionCheck(legacy, source)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        source.append("executionIncarnation", "inc-a").append("executionRevision", "current-target-revision");
        assertThat(MongoClusterRecoveryStore.originalExecutionCheck(legacy, source)).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
    }

    @Test
    void historicalStartupCannotCompleteAnExecutionWithACurrentFailureVerdict() {
        ClusterRecoverySuccessor successor = new ClusterRecoverySuccessor(PIPELINE, PROFILE, Set.of("node.alpha"),
                Set.of("crm"), NOW, "job-42", NOW, POSITIONS, null);
        ClusterRecoveryStartupReceipt receipt = new ClusterRecoveryStartupReceipt(PIPELINE, "job-42", NOW,
                Map.of("crm", witness()), POSITIONS, POSITIONS, NOW, false);
        Document source = new Document("executionGeneration", 42L).append("contextExecutionGeneration", 42L);
        Document actual = new Document("stateJson", "RUNNING");
        assertThat(MongoClusterRecoveryStore.startupExecutionCheck(successor, source, actual, receipt))
                .isEqualTo(ClusterRecoveryMutation.APPLIED);
        source.append("failureClaimGeneration", 8L);
        assertThat(MongoClusterRecoveryStore.startupExecutionCheck(successor, source, actual, receipt))
                .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        source.append("failureClaimGeneration", 0L);
        actual.append("stateJson", "FAILED");
        assertThat(MongoClusterRecoveryStore.startupExecutionCheck(successor, source, actual, receipt))
                .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
        actual.append("stateJson", "COMPLETED");
        assertThat(MongoClusterRecoveryStore.startupExecutionCheck(successor, source, actual, receipt))
                .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
    }

    @Test
    void missingRecordedSourceRequirementsCannotBecomeAKnownZeroSourcePlan() {
        ClusterRecoveryItem allocated = queued(POSITIONS)
                .permitted(new ClusterRecoveryPermit("permit-a", RECOVERY, NOW, NOW.plusSeconds(30), DEMAND, 0), NOW)
                .advanced(new ClusterRecoverySuccessor(PIPELINE, PROFILE, Set.of("node.alpha"), Set.of("crm"),
                        NOW, null, null, Map.of(), null), NOW);
        Document missing = ClusterRecoveryDocuments.item(allocated);
        missing.get("successor", Document.class).remove("requiredSourceIds");
        assertUnreadable(missing);
    }

    @Test
    void aPendingFirstFailureRoundTripsWithoutReleasingItsPermitOrLosingOriginalArguments() {
        ClusterRecoveryItem allocated = queued(POSITIONS)
                .permitted(new ClusterRecoveryPermit("permit-a", RECOVERY, NOW, NOW.plusSeconds(30), DEMAND, 0), NOW)
                .advanced(new ClusterRecoverySuccessor(PIPELINE, PROFILE, Set.of("node.alpha"), Set.of("crm"),
                        NOW, null, null, Map.of(), null), NOW);
        var diagnostic = new ClusterRecoveryDiagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                "connector.mongo.position-expired", Map.of("capture", "capture-a", "position", "resume:73"), POSITIONS,
                "Restore the retained source position before starting explicitly");
        ClusterRecoveryItem pending = allocated.failureNoted(new ClusterRecoveryFailureNote(PIPELINE,
                ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, diagnostic, NOW), NOW);
        ClusterRecoveryItem restored = ClusterRecoveryDocuments.item(Document.parse(ClusterRecoveryDocuments.item(pending).toJson()));
        assertThat(restored).isEqualTo(pending);
        assertThat(restored.permit()).isEqualTo(allocated.permit());
        assertThat(restored.successor().failureNote().diagnostic().params()).containsEntry("position", "resume:73");
        assertThat(restored.attempt()).isEqualTo(1);
    }

    private static void assertUnreadable(Document document) {
        assertThatThrownBy(() -> ClusterRecoveryDocuments.item(document)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    private static ClusterRecoveryItem queued(Map<String, ClusterRecoveryPosition> positions) {
        return ClusterRecoveryItem.enqueued(new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.MEMBER_LOSS, 41,
                "artifact-revision", 1L, PROFILE, PROFILE, 2, "intent", positions), 1, NOW, 3);
    }

    private static CaptureResumeWitness witness() {
        ChainPosition floor = POSITIONS.get("crm").position();
        return new CaptureResumeWitness("crm", "mongo", "capture-a", SrsConsumerId.of("orders", "crm").value(),
                ReadMode.CDC_ONLY, true, List.of("orders"), true, 9, floor, true, true, List.of(), null, 0,
                ConsumerProgressKind.SRS, floor, Map.of("orders", floor));
    }

    private static WorkloadClaimFence fence(WorkloadClaimType type, String resource, long execution, long topology) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", type, resource), new WorkloadOwner("node.alpha", "boot-a"),
                7, execution, topology, 1);
    }
}
