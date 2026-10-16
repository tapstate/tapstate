package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.model.ReadMode;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The durable recovery records refuse identities and steps that no real recovery could have written. */
class ClusterRecoveryRecordsTest {
    private static final Instant NOW = Instant.parse("2026-10-12T00:00:00Z");
    private static final ClusterRecoveryKey KEY = new ClusterRecoveryKey("east", "orders", "incarnation-a");
    private static final ClusterExecutionProfile PROFILE = profile("east", 1);
    private static final Map<String, ClusterCapacityDemand> DEMAND =
            Map.of("node-a", new ClusterCapacityDemand(3, 1, 1, 2, 200, 400));
    private static final ClusterRecoveryPosition POSITION = new ClusterRecoveryPosition("crm", "mongo", "capture-orders",
            ClusterRecoveryPosition.Kind.DURABLE_POSITION, new ChainPosition(new SourceOrder(7, 93), "resume-93"),
            "capture-majority-read", "srs/crm/7");

    @Test
    void aRecoveryEventNamesOneClusterAndNeverMovesItsProfileBackwards() {
        assertThat(event(1, "revision", 1L, PROFILE, false, PROFILE, 2, ClusterRecoveryCause.MEMBER_LOSS).uniqueKey().cause())
                .isEqualTo(ClusterRecoveryCause.MEMBER_LOSS);
        assertThat(event(1, null, null, null, true, profile("east", 2), 2, ClusterRecoveryCause.FULL_CLUSTER_RESTART)
                .legacySourceProfile()).isTrue();
        Map<String, ThrowingCallable> invalid = new LinkedHashMap<>();
        invalid.put("no original execution", () -> event(0, "revision", 1L, PROFILE, false, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("no target topology", () -> event(1, "revision", 1L, PROFILE, false, PROFILE, 0,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("target in another cluster", () -> event(1, "revision", 1L, PROFILE, false, profile("west", 1), 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("legacy marker with a known source profile", () -> event(1, "revision", 1L, PROFILE, true,
                profile("east", 2), 2, ClusterRecoveryCause.FULL_CLUSTER_RESTART));
        invalid.put("current event without its revision", () -> event(1, null, 1L, PROFILE, false, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("current event without its topology", () -> event(1, "revision", null, PROFILE, false, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("non-positive source topology", () -> event(1, "revision", 0L, PROFILE, false, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("source in another cluster", () -> event(1, "revision", 1L, profile("west", 1), false, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("target behind the source", () -> event(1, "revision", 1L, profile("east", 2), false, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        invalid.put("legacy member loss", () -> event(1, null, null, null, true, PROFILE, 2,
                ClusterRecoveryCause.MEMBER_LOSS));
        assertThat(invalid).hasSize(10);
        invalid.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("recovery event generations or cluster identities are invalid"));
        assertThatThrownBy(() -> new ClusterRecoveryEvent(KEY, ClusterRecoveryCause.MEMBER_LOSS, 1, "revision", 1L,
                PROFILE, PROFILE, 2, "intent", Map.of("other", POSITION)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("resume position keys must be source identities");
    }

    @Test
    void aCapacityReservationBelongsToOnePipelineOfItsCluster() {
        assertThat(reservation("reservation-1", "east", "orders", pipelineClaim("east", "orders"), 7L, "job-7")
                .nativeJobId()).isEqualTo("job-7");
        WorkloadClaimFence orders = pipelineClaim("east", "orders");
        WorkloadClaimFence capture = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "orders"), owner(), 1, 0, 2, 1);
        Map<String, ThrowingCallable> invalid = new LinkedHashMap<>();
        invalid.put("blank reservation", () -> reservation(" ", "east", "orders", orders, null, null));
        invalid.put("blank cluster", () -> reservation("reservation-1", " ", "orders", orders, null, null));
        invalid.put("blank pipeline", () -> reservation("reservation-1", "east", "", orders, null, null));
        invalid.put("blank incarnation", () -> new ClusterCapacityReservation("reservation-1", "east", "orders", " ",
                "intent", PROFILE, orders, DEMAND, NOW, NOW.plusSeconds(30), null, null));
        invalid.put("blank intent", () -> new ClusterCapacityReservation("reservation-1", "east", "orders",
                "incarnation", "", PROFILE, orders, DEMAND, NOW, NOW.plusSeconds(30), null, null));
        invalid.put("no demand", () -> new ClusterCapacityReservation("reservation-1", "east", "orders",
                "incarnation", "intent", PROFILE, orders, Map.of(), NOW, NOW.plusSeconds(30), null, null));
        invalid.put("deadline not after reservation", () -> new ClusterCapacityReservation("reservation-1", "east",
                "orders", "incarnation", "intent", PROFILE, orders, DEMAND, NOW, NOW, null, null));
        invalid.put("profile of another cluster", () -> new ClusterCapacityReservation("reservation-1", "east",
                "orders", "incarnation", "intent", profile("west", 1), orders, DEMAND, NOW, NOW.plusSeconds(30), null, null));
        invalid.put("claim of another cluster", () -> reservation("reservation-1", "east", "orders",
                pipelineClaim("west", "orders"), null, null));
        invalid.put("claim of another pipeline", () -> reservation("reservation-1", "east", "orders",
                pipelineClaim("east", "payments"), null, null));
        invalid.put("capture claim", () -> reservation("reservation-1", "east", "orders", capture, null, null));
        invalid.put("non-positive execution", () -> reservation("reservation-1", "east", "orders", orders, 0L, null));
        invalid.put("blank native job", () -> reservation("reservation-1", "east", "orders", orders, 7L, " "));
        invalid.put("native job without execution", () -> reservation("reservation-1", "east", "orders", orders,
                null, "job-7"));
        assertThat(invalid).hasSize(14);
        invalid.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("capacity reservation identity or step is invalid"));
    }

    @Test
    void aRecoveryPermitIsHeldByTheClusterRecoveryAuthorityAndTransfersOnce() {
        ClusterRecoveryPermit permit = permit(recoveryClaim("east", "east", 1), NOW.plusSeconds(30), DEMAND, 0);
        Map<String, ThrowingCallable> invalid = new LinkedHashMap<>();
        invalid.put("pipeline claim", () -> permit(pipelineClaim("east", "orders"), NOW.plusSeconds(30), DEMAND, 0));
        invalid.put("no profile generation", () -> permit(recoveryClaim("east", "east", 0), NOW.plusSeconds(30), DEMAND, 0));
        invalid.put("claim of one resource", () -> permit(recoveryClaim("east", "orders", 1), NOW.plusSeconds(30), DEMAND, 0));
        invalid.put("deadline not after reservation", () -> permit(recoveryClaim("east", "east", 1), NOW, DEMAND, 0));
        invalid.put("no demand", () -> permit(recoveryClaim("east", "east", 1), NOW.plusSeconds(30), Map.of(), 0));
        invalid.put("blank member", () -> permit(recoveryClaim("east", "east", 1), NOW.plusSeconds(30),
                Map.of(" ", DEMAND.get("node-a")), 0));
        invalid.put("negative transfer", () -> permit(recoveryClaim("east", "east", 1), NOW.plusSeconds(30), DEMAND, -1));
        assertThat(invalid).hasSize(7);
        invalid.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class).hasMessage("recovery permit fields are invalid"));

        ClusterRecoveryPermit handed = permit.handover(recoveryClaim("east", "east", 2));
        assertThat(handed.reservationId()).isEqualTo(permit.reservationId());
        assertThat(handed.recoveryClaim().profileGeneration()).isEqualTo(2);
        assertThatThrownBy(() -> permit.handover(recoveryClaim("west", "west", 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("handover must retain the recovery resource");
        assertThat(permit.transferred(42).transferredExecutionGeneration()).isEqualTo(42);
        assertThatThrownBy(() -> permit.transferred(0)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reservation can transfer only once to a real execution");
        assertThatThrownBy(() -> permit.transferred(42).transferred(43)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reservation can transfer only once to a real execution");
    }

    @Test
    void aCaptureReadAttemptNeedsItsCaptureAuthorityAndTheCoordinatesItRequested() {
        WorkloadClaimFence capture = captureClaim(1);
        assertThat(attempt("chain", 4, 1, capture, List.of("orders"), CaptureReadAttempt.Kind.RESUME, "token", null)
                .requestedToken()).isEqualTo("token");
        assertThat(attempt("chain", 4, 1, capture, List.of("orders"), CaptureReadAttempt.Kind.AT, null, NOW)
                .requestedInstant()).isEqualTo(NOW);
        Map<String, ThrowingCallable> invalid = new LinkedHashMap<>();
        invalid.put("blank chain", () -> attempt(" ", 4, 1, capture, List.of("orders"), CaptureReadAttempt.Kind.PRESENT, null, null));
        invalid.put("no chain epoch", () -> attempt("chain", 0, 1, capture, List.of("orders"), CaptureReadAttempt.Kind.PRESENT, null, null));
        invalid.put("no version", () -> attempt("chain", 4, 0, capture, List.of("orders"), CaptureReadAttempt.Kind.PRESENT, null, null));
        invalid.put("no tables", () -> attempt("chain", 4, 1, capture, List.of(), CaptureReadAttempt.Kind.PRESENT, null, null));
        invalid.put("pipeline authority", () -> attempt("chain", 4, 1, pipelineClaim("east", "orders"), List.of("orders"),
                CaptureReadAttempt.Kind.PRESENT, null, null));
        invalid.put("no profile generation", () -> attempt("chain", 4, 1, captureClaim(0), List.of("orders"),
                CaptureReadAttempt.Kind.PRESENT, null, null));
        invalid.put("resume without token", () -> attempt("chain", 4, 1, capture, List.of("orders"),
                CaptureReadAttempt.Kind.RESUME, null, null));
        invalid.put("resume with blank token", () -> attempt("chain", 4, 1, capture, List.of("orders"),
                CaptureReadAttempt.Kind.RESUME, " ", null));
        invalid.put("instant read without instant", () -> attempt("chain", 4, 1, capture, List.of("orders"),
                CaptureReadAttempt.Kind.AT, null, null));
        assertThat(invalid).hasSize(9);
        invalid.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("capture read attempt requires real authority and requested coordinates"));
    }

    @Test
    void aReaderIsAcceptedOnlyOnceItsResolvedAnchorDeliveredWithoutFailure() {
        CaptureReadAttempt attempt = attempt("chain", 4, 1, captureClaim(1), List.of("orders"),
                CaptureReadAttempt.Kind.PRESENT, null, null);
        assertThat(new CaptureReadState(attempt, "anchor", NOW, NOW, false, null).accepted()).isTrue();
        assertThat(new CaptureReadState(attempt, "anchor", NOW, null, false, null).accepted()).isFalse();
        assertThat(new CaptureReadState(attempt, null, null, null, false, null).accepted()).isFalse();
        assertThat(new CaptureReadState(attempt, "anchor", NOW, NOW, true, "engine.capture-failed", Map.of("table", "orders"),
                "RETRY", NOW).accepted()).isFalse();

        Map<String, ThrowingCallable> healthyFailure = new LinkedHashMap<>();
        healthyFailure.put("code", () -> new CaptureReadState(attempt, null, null, null, false, "engine.capture-failed"));
        healthyFailure.put("parameters", () -> new CaptureReadState(attempt, null, null, null, false, null,
                Map.of("table", "orders"), null, null));
        healthyFailure.put("disposition", () -> new CaptureReadState(attempt, null, null, null, false, null,
                Map.of(), "RETRY", null));
        healthyFailure.put("failure time", () -> new CaptureReadState(attempt, null, null, null, false, null,
                Map.of(), null, NOW));
        assertThat(healthyFailure).hasSize(4);
        healthyFailure.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class).hasMessage("healthy reader state cannot contain a failure"));

        Map<String, ThrowingCallable> detachedAnchor = new LinkedHashMap<>();
        detachedAnchor.put("anchor without its time", () -> new CaptureReadState(attempt, "anchor", null, null, false, null));
        detachedAnchor.put("time without its anchor", () -> new CaptureReadState(attempt, null, NOW, null, false, null));
        detachedAnchor.put("delivery without an anchor", () -> new CaptureReadState(attempt, null, null, NOW, false, null));
        assertThat(detachedAnchor).hasSize(3);
        detachedAnchor.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("resolved anchor and its observation travel together"));
    }

    @Test
    void aStartupReceiptNamesOneExecutionAndKeysEveryFactBySource() {
        WorkloadClaimFence execution = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), owner(), 7, 42, 2, 1);
        Map<String, ClusterRecoveryPosition> positions = Map.of("crm", POSITION);
        assertThat(receipt(execution, Map.of("crm", witness()), positions, positions).acceptedPositions())
                .containsOnlyKeys("crm");
        Map<String, ThrowingCallable> notExecution = new LinkedHashMap<>();
        notExecution.put("recovery authority", () -> receipt(recoveryClaim("east", "east", 1), Map.of(), Map.of(), Map.of()));
        notExecution.put("no profile generation", () -> receipt(new WorkloadClaimFence(execution.key(), owner(),
                7, 42, 2, 0), Map.of(), Map.of(), Map.of()));
        notExecution.put("no execution", () -> receipt(new WorkloadClaimFence(execution.key(), owner(), 7, 0, 2, 1),
                Map.of(), Map.of(), Map.of()));
        assertThat(notExecution).hasSize(3);
        notExecution.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("startup receipt requires an exact pipeline execution claim"));
        assertThatThrownBy(() -> receipt(execution, Map.of("other", witness()), positions, positions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("prepared witness keys must be source identities");
        assertThatThrownBy(() -> receipt(execution, Map.of("crm", witness()), Map.of("other", POSITION), positions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("receipt position keys must be source identities");
        assertThatThrownBy(() -> receipt(execution, Map.of("crm", witness()), positions, Map.of("other", POSITION)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("receipt position keys must be source identities");
    }

    @Test
    void queueAndPipelineFencesNameTheAuthorityOfTheirOwnClusterAndPipeline() {
        WorkloadClaimFence recovery = recoveryClaim("east", "east", 1);
        assertThat(new ClusterRecoveryFence(KEY, 0, "intent", PROFILE, 41, recovery).itemRevision()).isZero();
        Map<String, ThrowingCallable> queue = new LinkedHashMap<>();
        queue.put("negative revision", () -> new ClusterRecoveryFence(KEY, -1, "intent", PROFILE, 41, recovery));
        queue.put("no execution", () -> new ClusterRecoveryFence(KEY, 1, "intent", PROFILE, 0, recovery));
        queue.put("profile of another cluster", () -> new ClusterRecoveryFence(KEY, 1, "intent", profile("west", 1), 41,
                recovery));
        queue.put("authority of another cluster", () -> new ClusterRecoveryFence(KEY, 1, "intent", PROFILE, 41,
                recoveryClaim("west", "west", 1)));
        queue.put("pipeline authority", () -> new ClusterRecoveryFence(KEY, 1, "intent", PROFILE, 41,
                pipelineClaim("east", "east")));
        queue.put("authority over one resource", () -> new ClusterRecoveryFence(KEY, 1, "intent", PROFILE, 41,
                recoveryClaim("east", "orders", 1)));
        assertThat(queue).hasSize(6);
        queue.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("recovery fence identities or generations are invalid"));

        WorkloadClaimFence execution = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), owner(), 7, 42, 2, 1);
        assertThat(new ClusterRecoveryPipelineFence(KEY, 1, "intent", PROFILE, execution).pipelineClaim())
                .isEqualTo(execution);
        Map<String, ThrowingCallable> pipeline = new LinkedHashMap<>();
        pipeline.put("unwritten revision", () -> new ClusterRecoveryPipelineFence(KEY, 0, "intent", PROFILE, execution));
        pipeline.put("recovery authority", () -> new ClusterRecoveryPipelineFence(KEY, 1, "intent", PROFILE, recovery));
        pipeline.put("no execution", () -> new ClusterRecoveryPipelineFence(KEY, 1, "intent", PROFILE,
                pipelineClaim("east", "orders")));
        pipeline.put("execution of another profile", () -> new ClusterRecoveryPipelineFence(KEY, 1, "intent",
                profile("east", 2), execution));
        pipeline.put("profile of another cluster", () -> new ClusterRecoveryPipelineFence(KEY, 1, "intent",
                profile("west", 1), execution));
        pipeline.put("execution of another cluster", () -> new ClusterRecoveryPipelineFence(KEY, 1, "intent", PROFILE,
                new WorkloadClaimFence(new WorkloadClaimKey("west", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                        owner(), 7, 42, 2, 1)));
        pipeline.put("execution of another pipeline", () -> new ClusterRecoveryPipelineFence(KEY, 1, "intent", PROFILE,
                new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "payments"),
                        owner(), 7, 42, 2, 1)));
        assertThat(pipeline).hasSize(7);
        pipeline.forEach((name, call) -> assertThatThrownBy(call).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pipeline failure report requires the exact allocated recovery execution"));
    }

    @Test
    void aStartupFailureCarriesTheCodedFailureOfItsOwnProfilesReader() {
        WorkloadClaimFence execution = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), owner(), 7, 42, 2, 1);
        CaptureReadAttempt attempt = attempt("chain", 4, 1, captureClaim(1), List.of("orders"),
                CaptureReadAttempt.Kind.PRESENT, null, null);
        CaptureReadState failed = new CaptureReadState(attempt, null, null, null, true, "connector.mongo.position-expired",
                Map.of("table", "orders"), "RESTORE_POSITION", NOW);
        CaptureStartupFailure failure = new CaptureStartupFailure(execution, witness(), POSITION, NOW, failed);
        assertThat(failure.code()).isEqualTo("connector.mongo.position-expired");
        assertThat(failure.params()).containsOnly(Map.entry("table", "orders"));
        assertThat(failure.disposition()).isEqualTo("RESTORE_POSITION");
        assertThat(failure.failedAt()).isEqualTo(NOW);
        assertThat(failure.connectorId()).isEqualTo("mongo");
        assertThat(failure.captureId()).isEqualTo("chain");

        assertThatThrownBy(() -> new CaptureStartupFailure(new WorkloadClaimFence(
                new WorkloadClaimKey("west", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), owner(), 7, 42, 2, 1),
                witness(), POSITION, NOW, failed)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reader failure authority must belong to this execution profile");
        assertThatThrownBy(() -> new CaptureStartupFailure(new WorkloadClaimFence(execution.key(), owner(), 7, 42, 2, 2),
                witness(), POSITION, NOW, failed)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("reader failure authority must belong to this execution profile");
        Map<String, CaptureReadState> uncoded = new LinkedHashMap<>();
        uncoded.put("healthy reader", new CaptureReadState(attempt, null, null, null, false, null));
        uncoded.put("no code", new CaptureReadState(attempt, null, null, null, true, null, Map.of(), "RETRY", NOW));
        uncoded.put("no failure time", new CaptureReadState(attempt, null, null, null, true, "engine.capture-failed",
                Map.of(), "RETRY", null));
        uncoded.put("no disposition", new CaptureReadState(attempt, null, null, null, true, "engine.capture-failed",
                Map.of(), null, NOW));
        assertThat(uncoded).hasSize(4);
        uncoded.forEach((name, state) -> assertThatThrownBy(() -> new CaptureStartupFailure(execution, witness(),
                POSITION, NOW, state)).as(name).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("a qualified startup failure requires the actual coded reader failure"));
    }

    private static ClusterRecoveryEvent event(long original, String revision, Long sourceTopology,
            ClusterExecutionProfile source, boolean legacy, ClusterExecutionProfile target, long targetTopology,
            ClusterRecoveryCause cause) {
        return new ClusterRecoveryEvent(KEY, cause, original, revision, sourceTopology, source, legacy, target,
                targetTopology, "intent", Map.of("crm", POSITION));
    }

    private static ClusterCapacityReservation reservation(String id, String cluster, String pipeline,
            WorkloadClaimFence claim, Long execution, String job) {
        return new ClusterCapacityReservation(id, cluster, pipeline, "incarnation", "intent", PROFILE, claim, DEMAND,
                NOW, NOW.plusSeconds(30), execution, job);
    }

    private static ClusterRecoveryPermit permit(WorkloadClaimFence claim, Instant deadline,
            Map<String, ClusterCapacityDemand> demand, long transferred) {
        return new ClusterRecoveryPermit("reservation-1", claim, NOW, deadline, demand, transferred);
    }

    private static CaptureReadAttempt attempt(String chain, long epoch, long version, WorkloadClaimFence claim,
            List<String> tables, CaptureReadAttempt.Kind kind, String token, Instant at) {
        return new CaptureReadAttempt(chain, epoch, version, claim, tables, kind, token, at, NOW);
    }

    private static ClusterRecoveryStartupReceipt receipt(WorkloadClaimFence claim,
            Map<String, CaptureResumeWitness> witnesses, Map<String, ClusterRecoveryPosition> requested,
            Map<String, ClusterRecoveryPosition> accepted) {
        return new ClusterRecoveryStartupReceipt(claim, "job-42", NOW, witnesses, requested, accepted, NOW, false);
    }

    private static CaptureResumeWitness witness() {
        return new CaptureResumeWitness("crm", "mongo", "capture-orders", "orders-crm", ReadMode.CDC_ONLY, true,
                List.of("orders"), false, 0, null, false, false, List.of(), null, 0, null, null, Map.of());
    }

    private static ClusterExecutionProfile profile(String cluster, long generation) {
        return new ClusterExecutionProfile(cluster, generation, new ExecutionProfile(1, Map.of("build", "one")));
    }

    private static WorkloadOwner owner() {
        return new WorkloadOwner("node-a", "boot-a");
    }

    private static WorkloadClaimFence pipelineClaim(String cluster, String pipeline) {
        return new WorkloadClaimFence(new WorkloadClaimKey(cluster, WorkloadClaimType.PIPELINE_ACTUATION, pipeline),
                owner(), 7, 0, 2, 1);
    }

    private static WorkloadClaimFence recoveryClaim(String cluster, String resource, long profileGeneration) {
        return new WorkloadClaimFence(new WorkloadClaimKey(cluster, WorkloadClaimType.CLUSTER_RECOVERY, resource),
                owner(), 1, 0, 2, profileGeneration);
    }

    private static WorkloadClaimFence captureClaim(long profileGeneration) {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "chain"),
                owner(), 1, 0, 2, profileGeneration);
    }
}
