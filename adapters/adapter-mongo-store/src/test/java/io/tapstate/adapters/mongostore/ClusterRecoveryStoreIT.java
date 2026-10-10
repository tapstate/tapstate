package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryFence;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryMutation;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryPipelineFence;
import io.tapstate.spi.store.ClusterRecoveryDiagnostic;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ClusterRecoveryStartupReceipt;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Mongo transactions witness queue competition, durable fault windows and shared budget transfer. */
@RequiresDocker
class ClusterRecoveryStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Duration TTL = Duration.ofMinutes(5);
    private static final Duration PERMIT_TTL = Duration.ofSeconds(30);
    private static final Duration BACKOFF = Duration.ofMinutes(1);
    private static final Set<String> LIVE = Set.of("a", "b");
    private static final Map<String, ClusterCapacityDemand> DEMAND = Map.of("a", demand(1), "b", demand(1));
    private static final ClusterCapacityLimits LIMITS = new ClusterCapacityLimits(20, 20, 20, 20, 20, 20);
    private static final Map<String, ClusterRecoveryPosition> POSITIONS = Map.of("crm",
            new ClusterRecoveryPosition("crm", "mongo", "capture-crm", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                    new ChainPosition(new SourceOrder(3, 7), "resume-7"), "capture-majority-read", "srs/crm/3"));

    @Test
    void concurrentDuplicateEventsCommitOneSequenceAndOneItem() throws Exception {
        try (Fixture fixture = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            CountDownLatch start = new CountDownLatch(1);
            var one = workers.submit(() -> { start.await(); return fixture.enqueue(pipeline); });
            var two = workers.submit(() -> { start.await(); return fixture.enqueue(pipeline); });
            start.countDown();
            List<ClusterRecoveryStore.Result> results = List.of(one.get(30, TimeUnit.SECONDS), two.get(30, TimeUnit.SECONDS));
            assertThat(results).extracting(ClusterRecoveryStore.Result::outcome)
                    .containsExactlyInAnyOrder(ClusterRecoveryMutation.APPLIED, ClusterRecoveryMutation.DUPLICATE);
            assertThat(results.getFirst().item()).isEqualTo(results.getLast().item());
            assertThat(fixture.queue.countDocuments()).isEqualTo(1);
            assertThat(fixture.profileDocuments.find(new Document("_id", "east")).first().getLong("recoveryEnqueueSequence"))
                    .isEqualTo(1);
        }
    }

    @Test
    void racingPipelinesCannotAcquireTwoRecoverySlotsAndFifoWins() throws Exception {
        try (Fixture fixture = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            ClusterRecoveryItem first = fixture.enqueue(fixture.pipeline("orders", true)).item();
            ClusterRecoveryItem second = fixture.enqueue(fixture.pipeline("payments", true)).item();
            CountDownLatch start = new CountDownLatch(1);
            var one = workers.submit(() -> { start.await(); return fixture.permit(first, DEMAND, LIMITS); });
            var two = workers.submit(() -> { start.await(); return fixture.permit(second, DEMAND, LIMITS); });
            start.countDown();
            assertThat(one.get(30, TimeUnit.SECONDS).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(two.get(30, TimeUnit.SECONDS).outcome()).isEqualTo(ClusterRecoveryMutation.WAITING_PERMIT);
            assertThat(fixture.queue.countDocuments(new Document("permit", new Document("$ne", null)))).isEqualTo(1);
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void coordinatorHandoverResumesPermitAdvanceSubmissionAndStartupWithoutAnotherExecution() {
        for (int faultStep = 0; faultStep < 4; faultStep++) {
            try (Fixture fixture = new Fixture()) {
                Pipeline pipeline = fixture.pipeline("orders", true);
                ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
                if (faultStep >= 1) {
                    item = fixture.advance(item, pipeline.claim).item();
                }
                if (faultStep >= 2) {
                    item = fixture.submitted(item).item();
                }
                if (faultStep >= 3) {
                    fixture.running(pipeline.key.pipelineId());
                    item = fixture.initialized(item).item();
                }
                int attempt = item.attempt();
                long frontier = item.executionFrontier();
                WorkloadClaimFence oldRecovery = WorkloadClaimFence.from(fixture.recovery);
                assertThat(fixture.workloads.release(fixture.recovery)).isTrue();
                fixture.recovery = fixture.workloads.acquire(recoveryKey(), new WorkloadOwner("b", "boot-b"), 2, TTL).claim();
                item = fixture.store.resumePermit(fixture.fence(item), item.permit().reservationId(), PERMIT_TTL).item();
                assertThat(item.attempt()).isEqualTo(attempt);
                assertThat(item.executionFrontier()).isEqualTo(frontier);
                ClusterRecoveryFence staleOwner = new ClusterRecoveryFence(item.event().key(), item.itemRevision(),
                        item.event().intentFingerprint(), item.targetProfile(), item.executionFrontier(), oldRecovery);
                assertThat(fixture.store.complete(staleOwner).outcome()).isEqualTo(ClusterRecoveryMutation.STALE_RECOVERY_CLAIM);
                if (faultStep == 0) {
                    item = fixture.advance(item, pipeline.claim).item();
                }
                WorkloadClaim current = fixture.workloads.read(pipeline.claim.key()).orElseThrow().claim();
                assertThat(fixture.advance(item, current).outcome()).isEqualTo(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED);
                if (faultStep < 2) {
                    item = fixture.submitted(item).item();
                }
                if (faultStep < 3) {
                    fixture.running(pipeline.key.pipelineId());
                    item = fixture.initialized(item).item();
                }
                ClusterRecoveryStore.Result recovered = fixture.store.complete(fixture.fence(item));
                assertThat(recovered.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
                assertThat(recovered.item().status()).isEqualTo(ClusterRecoveryStatus.RECOVERED);
                assertThat(fixture.workloads.read(pipeline.claim.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
                assertThat(recovered.item().attempt()).isEqualTo(1);
                assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
            }
        }
    }

    @Test
    void poisonedHeadBackoffAndExhaustionLeaveLaterPipelinesRunnable() {
        try (Fixture fixture = new Fixture()) {
            ClusterRecoveryItem poison = fixture.enqueue(fixture.pipeline("orders", true)).item();
            ClusterRecoveryItem healthy = fixture.enqueue(fixture.pipeline("payments", true)).item();
            Map<String, ClusterCapacityDemand> excessive = Map.of("a", demand(30), "b", demand(30));
            ClusterRecoveryStore.Result first = fixture.permit(poison, excessive, LIMITS);
            assertThat(first.outcome()).isEqualTo(ClusterRecoveryMutation.CAPACITY_REFUSED);
            poison = first.item();
            assertThat(poison.status()).isEqualTo(ClusterRecoveryStatus.RETRY_BACKOFF);
            assertThat(fixture.permit(poison, excessive, LIMITS).outcome()).isEqualTo(ClusterRecoveryMutation.RETRY_BACKOFF);
            assertThat(fixture.permit(healthy, DEMAND, LIMITS).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            for (int attempt = 2; attempt <= 3; attempt++) {
                fixture.queue.updateOne(new Document("_id", ClusterRecoveryDocuments.id(poison.event().key())),
                        List.of(new Document("$set", new Document("nextEligibleAt", "$$NOW"))));
                if (attempt == 2) {
                    // The held healthy slot is an independent concurrency refusal, not a poison attempt.
                    assertThat(fixture.permit(fixture.store.read(poison.event().key()).orElseThrow(), excessive, LIMITS).outcome())
                            .isEqualTo(ClusterRecoveryMutation.WAITING_PERMIT);
                    fixture.finishHealthy(healthy.event().key());
                }
                poison = fixture.permit(fixture.store.read(poison.event().key()).orElseThrow(), excessive, LIMITS).item();
            }
            assertThat(poison.status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
            assertThat(poison.attempt()).isEqualTo(3);
            assertThat(poison.successor()).isNull();
            assertThat(poison.diagnostic().reason()).isEqualTo(io.tapstate.spi.store.ClusterRecoveryDiagnostic.Reason.CAPACITY_REFUSED);
            assertThat(poison.diagnostic().code()).isEqualTo(io.tapstate.core.lifecycle.LifecycleError.CLUSTER_CAPACITY_REFUSED.code());
            assertThat(fixture.store.read(healthy.event().key()).orElseThrow().status()).isEqualTo(ClusterRecoveryStatus.RECOVERED);
            ClusterRecoveryItem next = fixture.enqueue(fixture.pipeline("shipments", true)).item();
            assertThat(fixture.permit(next, DEMAND, LIMITS).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
        }
    }

    @Test
    void belowCommittedMajorityDoesNotGrantAPermitOrSpendAnAttempt() {
        try (Fixture fixture = new Fixture()) {
            ClusterRecoveryItem item = fixture.enqueue(fixture.pipeline("orders", true, List.of(SourceRef.bare("crm")),
                    true, new WorkloadOwner("a", "boot-a"))).item();
            assertThat(fixture.workloads.release(fixture.nodeB)).isTrue();
            assertThat(fixture.permit(item, DEMAND, LIMITS).outcome()).isEqualTo(ClusterRecoveryMutation.WAITING_QUORUM);
            assertThat(fixture.store.read(item.event().key()).orElseThrow()).isEqualTo(item);
            assertThat(fixture.occupancy.countDocuments()).isZero();
            WorkloadClaim restored = fixture.profiles.reserve("east", new WorkloadOwner("b", "boot-b-next"),
                    URI.create("http://b:8080"), fixture.profile.profile(), TTL).node().registration().nodeSession();
            fixture.profiles.markJoined(restored, "uuid-b-next", "b:5701");
            assertThat(fixture.permit(item, DEMAND, LIMITS).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.read(item.event().key()).orElseThrow().attempt()).isZero();
        }
    }

    @Test
    void ordinaryStartAndRecoveryCountTheSameReservationBeforeAndAfterSubmission() {
        try (Fixture fixture = new Fixture()) {
            Pipeline normal = fixture.pipeline("normal", false);
            ClusterCapacityLimits one = new ClusterCapacityLimits(1, 1, 1, 1, 1, 1);
            ClusterCapacityStore.Result reserved = fixture.capacity.reserve(normal.claim, fixture.profile,
                    normal.key.incarnation(), normal.intentFingerprint, DEMAND, one, PERMIT_TTL);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(reserved.reservation().executionGeneration()).isNull();
            ClusterRecoveryItem recovering = fixture.enqueue(fixture.pipeline("orders", true)).item();
            assertThat(fixture.permit(recovering, DEMAND, one).outcome()).isEqualTo(ClusterRecoveryMutation.CAPACITY_REFUSED);
            ClusterCapacityStore.Result advanced = fixture.capacity.advanceExecution(reserved.reservation(), normal.claim, LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            Document execution = fixture.claims.find(new Document("resourceId", normal.key.pipelineId())).first();
            assertThat(execution.getString("executionIncarnation")).isEqualTo(normal.key.incarnation());
            assertThat(execution.getString("executionRevision")).isEqualTo(normal.event == null
                    ? fixture.artifacts.identity(normal.key.pipelineId()).orElseThrow().contentHash() : normal.event.originalExecutionRevision());
            assertThat(execution.getLong("executionTopologyRevision")).isEqualTo(2);
            assertThat(execution.get("executionProfile", Document.class).getString("hash")).isEqualTo(fixture.profile.profile().hash());
            ClusterCapacityStore.Result submitted = fixture.capacity.submitted(advanced.reservation(),
                    WorkloadClaimFence.from(advanced.advancedPipelineClaim()), "normal-job");
            assertThat(submitted.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(submitted.reservation().reservationId()).isEqualTo(reserved.reservation().reservationId());
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
        }
    }

    @Test
    void aRecoveredSuccessorCanEnqueueANewLossAtTheSameProfileAndCommittedRevision() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            item = fixture.submitted(fixture.advance(item, pipeline.claim).item()).item();
            fixture.running("orders");
            item = fixture.initialized(item).item();
            ClusterRecoveryItem recovered = fixture.store.complete(fixture.fence(item)).item();
            assertThat(recovered.status()).isEqualTo(ClusterRecoveryStatus.RECOVERED);
            assertThat(fixture.profiles.markJoined(fixture.nodeB, "uuid-b-rejoined", "b:5701")).isTrue();
            WorkloadClaim successor = fixture.workloads.read(pipeline.claim.key()).orElseThrow().claim();
            ClusterRecoveryEvent next = new ClusterRecoveryEvent(pipeline.key, ClusterRecoveryCause.MEMBER_LOSS,
                    successor.executionGeneration(), successor.executionRevision(), successor.executionTopologyRevision(),
                    successor.executionProfile(), fixture.profile, 2, pipeline.intentFingerprint, POSITIONS);
            ClusterRecoveryStore.Result queued = fixture.store.enqueue(next,
                    ClusterRecoveryFence.enqueue(next, WorkloadClaimFence.from(fixture.recovery)));
            assertThat(queued.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(queued.item().executionFrontier()).isEqualTo(successor.executionGeneration());
            assertThat(queued.item().attempt()).isZero();
            assertThat(fixture.store.list("east", 0, 10)).hasSize(2);
            assertThat(fixture.enqueue(pipeline).outcome()).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            assertThat(fixture.store.read(pipeline.key).orElseThrow()).isEqualTo(queued.item());
        }
    }

    @Test
    void aRequiredSourceSubsetCannotCertifyTheAllocatedSuccessor() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true, List.of(SourceRef.bare("crm"), SourceRef.bare("erp")));
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            item = fixture.submitted(fixture.advance(item, pipeline.claim).item()).item();
            fixture.running("orders");
            assertThat(item.successor().requiredSourceIds()).containsExactlyInAnyOrder("crm", "erp");
            assertThat(fixture.initialized(item).outcome()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
            assertThat(fixture.store.complete(fixture.fence(item)).outcome()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
            assertThat(fixture.store.read(pipeline.key).orElseThrow().status()).isEqualTo(ClusterRecoveryStatus.REBUILDING);
        }
    }

    @Test
    void aCompleteReceiptMapCannotOverrideASmallerStoredPreparationSet() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true, List.of(SourceRef.bare("crm"), SourceRef.bare("erp")));
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            item = fixture.submitted(fixture.advance(item, pipeline.claim).item()).item();
            fixture.running("orders");
            ClusterRecoveryStartupReceipt partial = fixture.startupReceipt(item);
            CaptureResumeWitness crm = partial.preparedWitnesses().get("crm");
            CaptureResumeWitness erp = new CaptureResumeWitness("erp", crm.connectorId(), crm.miningChainId(),
                    SrsConsumerId.of("orders", "erp").value(), crm.readMode(), crm.srsEnabled(), crm.tables(),
                    crm.chainPresent(), crm.chainEpoch(), crm.sourceRead(), crm.sourceReadDurable(), crm.consumerPresent(),
                    crm.snapshotCompletedTables(), crm.cdcStartPosition(), crm.snapshotEpoch(), crm.progressKind(),
                    crm.sinkAcked(), crm.sinkAckedByTable());
            var point = partial.requestedPositions().get("crm");
            var extra = new ClusterRecoveryPosition("erp", point.connectorId(), point.captureId(), point.kind(),
                    point.position(), point.provenance(), point.durableStateReference());
            Map<String, ClusterRecoveryPosition> positions = Map.of("crm", point, "erp", extra);
            ClusterRecoveryStartupReceipt asserted = new ClusterRecoveryStartupReceipt(partial.pipelineClaim(),
                    partial.nativeJobId(), partial.nativeInitializedAt(), Map.of("crm", crm, "erp", erp),
                    positions, positions, partial.positionsAcceptedAt(), false);
            assertThat(item.startupReceiptCheck(asserted)).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.recordStartup(fixture.fence(item), asserted.pipelineClaim(), asserted).outcome())
                    .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
        }
    }

    @Test
    void aMovingCheckpointCannotReplaceTheFrozenActualSourceStart() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            item = fixture.submitted(fixture.advance(item, pipeline.claim).item()).item();
            fixture.running("orders");
            ClusterRecoveryStartupReceipt prepared = fixture.startupReceipt(item);
            fixture.meta.advanceCaptureCheckpoint("capture-crm", new ChainPosition(new SourceOrder(fixture.captureEpoch, 19), "resume-19"),
                    List.of("orders"));
            ClusterRecoveryPosition original = prepared.requestedPositions().get("crm");
            var later = new ClusterRecoveryPosition("crm", original.connectorId(), original.captureId(), original.kind(),
                    new ChainPosition(new SourceOrder(fixture.captureEpoch, 19), "resume-19"), original.provenance(), original.durableStateReference());
            ClusterRecoveryStartupReceipt fabricated = new ClusterRecoveryStartupReceipt(prepared.pipelineClaim(), prepared.nativeJobId(),
                    prepared.nativeInitializedAt(), prepared.preparedWitnesses(), Map.of("crm", later), Map.of("crm", later),
                    prepared.positionsAcceptedAt(), false);
            assertThat(item.startupReceiptCheck(fabricated)).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.recordStartup(fixture.fence(item), fabricated.pipelineClaim(), fabricated).outcome())
                    .isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
            var observed = fixture.store.recordStartup(fixture.fence(item), prepared.pipelineClaim(), prepared);
            assertThat(observed.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.complete(fixture.fence(observed.item())).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
        }
    }

    @Test
    void failedOrReleasedSuccessorsCannotRecoverFromAHistoricalStartupReceipt() {
        for (boolean released : List.of(false, true)) {
            try (Fixture fixture = new Fixture()) {
                Pipeline pipeline = fixture.pipeline("orders", true);
                ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
                item = fixture.submitted(fixture.advance(item, pipeline.claim).item()).item();
                fixture.running("orders");
                item = fixture.initialized(item).item();
                WorkloadClaim successor = fixture.workloads.read(pipeline.claim.key()).orElseThrow().claim();
                if (released) {
                    assertThat(fixture.workloads.release(successor)).isTrue();
                } else {
                    assertThat(fixture.workloads.recordExecutionFailure(successor, true)).isPresent();
                }
                assertThat(fixture.store.complete(fixture.fence(item)).outcome()).isEqualTo(released
                        ? ClusterRecoveryMutation.STALE_PIPELINE_CLAIM : ClusterRecoveryMutation.STALE_EXECUTION);
                assertThat(fixture.store.read(pipeline.key).orElseThrow().status()).isEqualTo(ClusterRecoveryStatus.REBUILDING);
                assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
            }
        }
    }

    @Test
    void ordinaryAllocationRecordsTheOriginalCohortAndTakeoverPreservesIt() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("normal", false);
            var reserved = fixture.capacity.reserve(pipeline.claim, fixture.profile, pipeline.key.incarnation(),
                    pipeline.intentFingerprint, DEMAND, LIMITS, PERMIT_TTL);
            var advanced = fixture.capacity.advanceExecution(reserved.reservation(), pipeline.claim, LIVE);
            WorkloadClaim original = advanced.advancedPipelineClaim();
            assertThat(original.executionMembers()).extractingByKey("b")
                    .isEqualTo(new io.tapstate.spi.store.ClusterExecutionMember("b", "boot-b", "uuid-b"));
            assertThat(fixture.workloads.release(original)).isTrue();
            WorkloadClaim inherited = fixture.workloads.acquire(original.key(), new WorkloadOwner("a", "boot-a"), 2, TTL).claim();
            assertThat(inherited.executionMembers()).isEqualTo(original.executionMembers());
            assertThat(fixture.profiles.markJoined(fixture.nodeB, "uuid-b-next", "b:5701")).isTrue();
            fixture.running("normal");
            ClusterRecoveryEvent loss = new ClusterRecoveryEvent(pipeline.key, ClusterRecoveryCause.MEMBER_LOSS,
                    inherited.executionGeneration(), inherited.executionRevision(), inherited.executionTopologyRevision(),
                    inherited.executionProfile(), fixture.profile, 2, pipeline.intentFingerprint, POSITIONS);
            assertThat(fixture.store.enqueue(loss, ClusterRecoveryFence.enqueue(loss, WorkloadClaimFence.from(fixture.recovery))).outcome())
                    .isEqualTo(ClusterRecoveryMutation.APPLIED);
        }
    }

    @Test
    void legacyColdRecoveryRequiresThePersistedIncarnationBaseline() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            fixture.running("orders");
            Document id = new Document("_id", MongoClusterCapacityStore.claimId("east", "orders"));
            fixture.claims.updateOne(id, new Document("$unset", new Document("executionProfileVersion", "")
                    .append("executionProfile", "").append("executionTopologyRevision", "").append("executionRevision", "")
                    .append("executionIncarnation", "").append("failureClaimGeneration", "").append("failureAfterMemberLoss", "")));
            ClusterRecoveryEvent legacy = new ClusterRecoveryEvent(pipeline.key, ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                    pipeline.claim.executionGeneration(), null, null, null, true, fixture.profile, 2,
                    pipeline.intentFingerprint, POSITIONS);
            ClusterRecoveryFence expected = ClusterRecoveryFence.enqueue(legacy, WorkloadClaimFence.from(fixture.recovery));
            assertThat(fixture.store.enqueue(legacy, expected).outcome()).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            fixture.claims.updateOne(id, new Document("$set", new Document("executionIncarnation", pipeline.key.incarnation())));
            assertThat(fixture.store.enqueue(legacy, expected).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.read(pipeline.key).orElseThrow().event().originalExecutionRevision()).isNull();
        }
    }

    @Test
    void firstRecoveryWaitsForTheOriginalCachedLeasePromiseWithoutSpendingAnAttempt() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true, List.of(SourceRef.bare("crm")), false);
            ClusterRecoveryItem item = fixture.enqueue(pipeline).item();
            Document current = fixture.claims.find(new Document("_id", MongoClusterCapacityStore.claimId("east", "orders"))).first();
            assertThat(current.getLong("claimGeneration")).isGreaterThan(current.getLong("executionClaimGeneration"));
            assertThat(current.getDate("retiredAuthorizationUntil").toInstant()).isAfter(item.updatedAt());
            ClusterRecoveryStore.Result permit = fixture.permit(item, DEMAND, LIMITS);
            assertThat(permit.outcome()).isEqualTo(ClusterRecoveryMutation.WAITING_PERMIT);
            assertThat(permit.item().attempt()).isZero();
            assertThat(fixture.occupancy.countDocuments()).isZero();
            assertThat(fixture.workloads.read(pipeline.claim.key()).orElseThrow().claim().executionGeneration()).isEqualTo(1);
        }
    }

    @Test
    void capacityReadDoesNotTouchProfileSerialsOrOccupancyRows() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("normal", false);
            var reserved = fixture.capacity.reserve(pipeline.claim, fixture.profile, pipeline.key.incarnation(),
                    pipeline.intentFingerprint, DEMAND, LIMITS, PERMIT_TTL);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            Document beforeProfile = fixture.profileDocuments.find(new Document("_id", "east")).first();
            List<Document> beforeRows = fixture.occupancy.find().into(new ArrayList<>());
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
            assertThat(fixture.profileDocuments.find(new Document("_id", "east")).first()).isEqualTo(beforeProfile);
            assertThat(fixture.occupancy.find().into(new ArrayList<>())).isEqualTo(beforeRows);
        }
    }

    @Test
    void retiredCapacityReadOmitsDemandWithoutDeletingItsStoredRow() {
        try (Fixture fixture = new Fixture()) {
            PipelineResource resource = new PipelineResource("retired", null, List.of(SourceRef.bare("crm")),
                    null, null, null, null, Map.of());
            fixture.artifacts.create(resource);
            ArtifactIdentity identity = fixture.artifacts.identity("retired").orElseThrow();
            DesiredState intent = new DesiredState("retired", PipelineState.RUNNING, identity.contentHash());
            fixture.desired.save(intent);
            fixture.states.create("retired", StateJson.of(PipelineState.STOPPED), java.time.Instant.now());
            WorkloadClaim claim = fixture.workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "retired"),
                    fixture.nodeB.owner(), 2, Duration.ofSeconds(3)).claim();
            var reserved = fixture.capacity.reserve(claim, fixture.profile, identity.incarnation(),
                    DesiredStateFingerprint.of(intent), DEMAND, LIMITS, Duration.ofSeconds(3));
            var advanced = fixture.capacity.advanceExecution(reserved.reservation(), claim, LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.capacity.submitted(advanced.reservation(), WorkloadClaimFence.from(advanced.advancedPipelineClaim()), "retired-job").outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.workloads.release(advanced.advancedPipelineClaim())).isTrue();
            fixture.awaitRetirement(claim.key());
            Document profile = fixture.profileDocuments.find(new Document("_id", "east")).first();
            List<Document> rows = fixture.occupancy.find().into(new ArrayList<>());
            assertThat(rows).hasSize(1);
            assertThat(fixture.capacity.occupied("east")).isEmpty();
            assertThat(fixture.occupancy.find().into(new ArrayList<>())).isEqualTo(rows);
            assertThat(fixture.profileDocuments.find(new Document("_id", "east")).first()).isEqualTo(profile);
        }
    }

    @Test
    void unknownExecutionDemandStillFailsAReadWithItsRegisteredCode() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("normal", false);
            fixture.workloads.advanceExecution(pipeline.claim, 2, LIVE).orElseThrow();
            fixture.running("normal");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.capacity.occupied("east"))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(io.tapstate.core.lifecycle.LifecycleError.CLUSTER_CAPACITY_REFUSED));
        }
    }

    @Test
    void aPipelineFailureNoteSurvivesMissingRecoveryOwnerWithoutReleasingDemand() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true, List.of(SourceRef.bare("crm")), true,
                    fixture.nodeB.owner(), Duration.ofSeconds(8));
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            WorkloadClaim execution = allocated.advancedPipelineClaim();
            fixture.running("orders");
            assertThat(fixture.workloads.recordExecutionFailure(execution, false)).isPresent();
            assertThat(fixture.workloads.release(fixture.recovery)).isTrue();
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(io.tapstate.core.lifecycle.LifecycleError.PIPELINE_NOT_RUNNABLE,
                            Map.of("pipeline", "orders"), null), POSITIONS, "Correct the reported failure before retrying");
            var recorded = fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null);
            assertThat(recorded.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(recorded.item().permit()).isEqualTo(item.permit());
            assertThat(recorded.item().attempt()).isEqualTo(1);
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
            fixture.recovery = fixture.workloads.acquire(recoveryKey(), new WorkloadOwner("a", "boot-a"), 2, TTL).claim();
            item = fixture.store.resumePermit(fixture.fence(recorded.item()), recorded.item().permit().reservationId(), PERMIT_TTL).item();
            var placeholder = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), POSITIONS, "Wait for retirement");
            assertThat(fixture.store.fail(fixture.fence(item), executionFence(execution), placeholder,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, BACKOFF).outcome())
                    .isEqualTo(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED);
            assertThat(fixture.workloads.release(fixture.workloads.read(execution.key()).orElseThrow().claim())).isTrue();
            fixture.awaitRetirement(execution.key());
            var failed = fixture.store.fail(fixture.fence(item), executionFence(execution), placeholder,
                    ClusterRecoveryStore.FailureStage.BEFORE_EXECUTION_ADVANCE, BACKOFF);
            assertThat(failed.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(failed.item().diagnostic()).isEqualTo(diagnostic);
            assertThat(failed.item().attempt()).isEqualTo(1);
            assertThat(failed.item().status()).isEqualTo(ClusterRecoveryStatus.RETRY_BACKOFF);
        }
    }

    @Test
    void unrecordedNativeFailureAndStalePipelineCannotReportFailureFacts() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = allocated.item();
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), POSITIONS, "Wait for retirement");
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome()).isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            assertThat(fixture.workloads.release(allocated.advancedPipelineClaim())).isTrue();
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome()).isEqualTo(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM);
            assertThat(fixture.store.read(item.event().key()).orElseThrow().successor().failureNote()).isNull();
        }
    }

    @Test
    void aLivePipelineCanReportItsFailureBelowDataAdmissionQuorum() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = allocated.item();
            assertThat(fixture.workloads.recordExecutionFailure(allocated.advancedPipelineClaim(), false)).isPresent();
            WorkloadClaim nodeA = fixture.workloads.read(new WorkloadClaimKey("east", WorkloadClaimType.NODE_SESSION, "a"))
                    .orElseThrow().claim();
            assertThat(fixture.workloads.release(nodeA)).isTrue();
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null),
                    POSITIONS, "Wait for exact authority retirement before retrying");
            var recorded = fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null);
            assertThat(recorded.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(recorded.item().permit()).isEqualTo(item.permit());
            assertThat(recorded.item().attempt()).isEqualTo(item.attempt());
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
            fixture.recovery = fixture.workloads.acquire(recoveryKey(), fixture.nodeB.owner(), 2, TTL).claim();
            assertThat(fixture.store.resumePermit(fixture.fence(recorded.item()), item.permit().reservationId(), PERMIT_TTL)
                    .outcome()).isEqualTo(ClusterRecoveryMutation.WAITING_QUORUM);
        }
    }

    @Test
    void aFailureReportCannotResetAnExhaustedPreAllocationBudget() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.enqueue(pipeline).item();
            Map<String, ClusterCapacityDemand> oversized = Map.of("a", demand(21), "b", demand(21));
            for (int attempt = 1; attempt <= item.maxAttempts(); attempt++) {
                fixture.awaitEligibility(item.event().key());
                var refused = fixture.store.acquirePermit(fixture.fence(item), oversized, LIMITS,
                        PERMIT_TTL, Duration.ofMillis(1), 1);
                assertThat(refused.outcome()).isEqualTo(ClusterRecoveryMutation.CAPACITY_REFUSED);
                item = refused.item();
                assertThat(item.attempt()).isEqualTo(attempt);
            }
            assertThat(item.status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
            var expected = new ClusterRecoveryPipelineFence(item.event().key(), item.itemRevision(),
                    item.event().intentFingerprint(), item.targetProfile(), WorkloadClaimFence.from(pipeline.claim));
            assertThat(fixture.store.recordFailureNote(expected, item.diagnostic(),
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome())
                    .isEqualTo(ClusterRecoveryMutation.TERMINAL);
            assertThat(fixture.store.read(item.event().key()).orElseThrow()).isEqualTo(item);
            assertThat(fixture.occupancy.countDocuments()).isZero();
        }
    }

    @Test
    void anActiveItemCannotReportFailureOverDurableFiniteCompletion() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = allocated.item();
            assertThat(fixture.workloads.recordExecutionFailure(allocated.advancedPipelineClaim(), false)).isPresent();
            var checkpoint = fixture.states.read("orders").orElseThrow();
            fixture.states.compareAndSwap("orders", checkpoint.epoch(), StateJson.of(PipelineState.COMPLETED), checkpoint.touchTime());
            assertThat(fixture.states.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.COMPLETED));
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null),
                    POSITIONS, "Inspect the completed execution");
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome())
                    .isEqualTo(ClusterRecoveryMutation.TERMINAL);
            assertThat(fixture.store.read(item.event().key()).orElseThrow()).isEqualTo(item);
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void theQualifiedSourceFailureRemainsTerminalAfterItsPipelineIsReleased() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true, List.of(SourceRef.bare("crm")), true,
                    fixture.nodeB.owner(), Duration.ofSeconds(8));
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            var receipt = fixture.startupReceipt(item);
            var reader = fixture.meta.captureReadState("capture-crm").orElseThrow();
            assertThat(fixture.meta.recordCaptureReadFailure(reader.attempt(), IoError.SRS_PROGRESS_UNPROVEN.code(),
                    Map.of("pipeline", "orders"), "manual-start-required")).isTrue();
            var fact = fixture.meta.captureStartupFailure(item.successor().pipelineClaim(), receipt.preparedWitnesses().get("crm")).orElseThrow();
            var diagnostic = new ClusterRecoveryDiagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                    fact.code(), fact.params(), Map.of("crm", fact.requestedPosition()), fact.disposition());
            var recorded = fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, fact);
            assertThat(recorded.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            item = recorded.item();
            assertThat(fixture.store.complete(fixture.fence(item)).outcome()).isEqualTo(ClusterRecoveryMutation.MISSING_STARTUP_RECEIPT);
            assertThat(fixture.workloads.release(allocated.advancedPipelineClaim())).isTrue();
            assertThat(fixture.meta.captureStartupFailure(item.successor().pipelineClaim(), fact.witness())).isEmpty();
            fixture.awaitRetirement(allocated.advancedPipelineClaim().key());
            var placeholder = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), POSITIONS, "Wait for retirement");
            var failed = fixture.store.fail(fixture.fence(item), item.successor().pipelineClaim(), placeholder,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, BACKOFF);
            assertThat(failed.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(failed.item().status()).isEqualTo(ClusterRecoveryStatus.REBUILD_FAILED);
            assertThat(failed.item().diagnostic()).isEqualTo(diagnostic);
            assertThat(failed.item().attempt()).isEqualTo(1);
            assertThat(fixture.occupancy.countDocuments()).isZero();
        }
    }

    private static WorkloadClaimFence executionFence(WorkloadClaim claim) {
        return WorkloadClaimFence.from(claim);
    }

    private static ClusterCapacityDemand demand(long processors) {
        return new ClusterCapacityDemand(processors, 0, 1, 1, 1, 1);
    }

    private static WorkloadClaimKey recoveryKey() {
        return new WorkloadClaimKey("east", WorkloadClaimType.CLUSTER_RECOVERY, "east");
    }

    private record Pipeline(ClusterRecoveryKey key, WorkloadClaim claim, ClusterRecoveryEvent event, String intentFingerprint) {}

    private static final class Fixture implements AutoCloseable {
        private final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        private final String database = "recovery_" + UUID.randomUUID().toString().replace("-", "");
        private final MongoCollection<Document> queue = collection("recovery_queue");
        private final MongoCollection<Document> occupancy = collection("capacity");
        private final MongoCollection<Document> claims = collection("claims");
        private final MongoCollection<Document> profileDocuments = collection("profiles");
        private final MongoCollection<Document> memberDocuments = collection("membership");
        private final MongoCollection<Document> nodeDocuments = collection("nodes");
        private final MongoCollection<Document> desiredDocuments = collection("desired");
        private final MongoCollection<Document> stateDocuments = collection("states");
        private final MongoCollection<Document> artifactDocuments = collection("artifacts");
        private final MongoSrsMetaStore meta = new MongoSrsMetaStore(client, collection("meta"), collection("consumers"), claims);
        private final MongoClusterProfileStore profiles = new MongoClusterProfileStore(client, profileDocuments, claims, nodeDocuments);
        private final MongoWorkloadClaimStore workloads = new MongoWorkloadClaimStore(claims, profiles);
        private final MongoClusterCapacityStore capacity = new MongoClusterCapacityStore(profiles, workloads, occupancy, claims,
                profileDocuments, memberDocuments, nodeDocuments, desiredDocuments, stateDocuments, artifactDocuments);
        private final MongoClusterRecoveryStore store = new MongoClusterRecoveryStore(queue, capacity, meta);
        private final MongoArtifactStore artifacts = new MongoArtifactStore(client, artifactDocuments);
        private final MongoDesiredStore desired = new MongoDesiredStore(desiredDocuments);
        private final MongoStateStore states = new MongoStateStore(stateDocuments);
        private final WorkloadClaim nodeB;
        private final ClusterExecutionProfile profile;
        private final WorkloadClaim capture;
        private final long captureEpoch;
        private WorkloadClaim recovery;

        private Fixture() {
            ExecutionProfile proposed = new ExecutionProfile(1, Map.of("build", "one", "threads", "4"));
            List<WorkloadClaim> members = new ArrayList<>();
            for (String node : List.of("a", "b", "c")) {
                WorkloadClaim claim = profiles.reserve("east", new WorkloadOwner(node, "boot-" + node),
                        URI.create("http://" + node + ":8080"), proposed, TTL).node().registration().nodeSession();
                profiles.markJoined(claim, "uuid-" + node, node + ":5701");
                members.add(claim);
            }
            nodeB = members.get(1);
            profile = profiles.profile("east").orElseThrow();
            memberDocuments.insertOne(new Document("_id", "east").append("profileGeneration", profile.generation())
                    .append("revision", 2L).append("activeNodeIds", List.of("a", "b", "c")));
            workloads.release(members.get(2));
            recovery = workloads.acquire(recoveryKey(), members.getFirst().owner(), 2, TTL).claim();
            capture = workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "capture-crm"),
                    members.getFirst().owner(), 2, TTL).claim();
            meta.create("capture-crm", null);
            meta.openEpoch("capture-crm");
            meta.openEpoch("capture-crm");
            captureEpoch = meta.openEpoch("capture-crm");
            assertThat(meta.publishCaptureTables("capture-crm", captureEpoch, List.of("orders"))).isTrue();
            meta.advanceCaptureCheckpoint("capture-crm", new ChainPosition(new SourceOrder(captureEpoch, 7), "resume-7"), List.of("orders"));
        }

        private MongoCollection<Document> collection(String name) {
            return client.getDatabase(database).getCollection(name);
        }

        private Pipeline pipeline(String id, boolean oldExecution) {
            return pipeline(id, oldExecution, List.of(SourceRef.bare("crm")));
        }

        private Pipeline pipeline(String id, boolean oldExecution, List<SourceRef> sources) {
            return pipeline(id, oldExecution, sources, true);
        }

        private Pipeline pipeline(String id, boolean oldExecution, List<SourceRef> sources, boolean awaitOriginalRetirement) {
            return pipeline(id, oldExecution, sources, awaitOriginalRetirement, nodeB.owner());
        }

        private Pipeline pipeline(String id, boolean oldExecution, List<SourceRef> sources,
                boolean awaitOriginalRetirement, WorkloadOwner owner) {
            return pipeline(id, oldExecution, sources, awaitOriginalRetirement, owner, TTL);
        }

        private Pipeline pipeline(String id, boolean oldExecution, List<SourceRef> sources,
                boolean awaitOriginalRetirement, WorkloadOwner owner, Duration liveTtl) {
            PipelineResource resource = new PipelineResource(id, null, sources,
                    null, null, null, null, Map.of());
            artifacts.create(resource);
            ArtifactIdentity identity = artifacts.identity(id).orElseThrow();
            DesiredState intent = new DesiredState(id, PipelineState.RUNNING, identity.contentHash());
            desired.save(intent);
            states.create(id, StateJson.of(oldExecution ? PipelineState.FAILED : PipelineState.STOPPED), java.time.Instant.now());
            WorkloadClaim claim = workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, id),
                    owner, oldExecution ? 1 : 2, oldExecution
                            ? Duration.ofSeconds(awaitOriginalRetirement ? 3 : 20) : liveTtl).claim();
            if (oldExecution) {
                claim = workloads.advanceExecution(claim, 1, Set.of("a", "b", "c")).orElseThrow();
                claims.updateOne(WorkloadClaimDocuments.live(WorkloadClaimFence.from(claim)),
                        new Document("$set", new Document("executionIncarnation", identity.incarnation())
                                .append("executionRevision", identity.contentHash())
                                .append("executionMembers", List.of(
                                        new Document("nodeId", "a").append("bootId", "boot-a").append("memberUuid", "uuid-a"),
                                        new Document("nodeId", "b").append("bootId", "boot-b").append("memberUuid", "uuid-b"),
                                        new Document("nodeId", "c").append("bootId", "boot-c").append("memberUuid", "uuid-c")))));
                claim = workloads.recordExecutionFailure(claim, true).orElseThrow();
                claim = workloads.acquire(claim.key(), owner, 2,
                        Duration.ofSeconds(awaitOriginalRetirement ? 3 : 20)).claim();
                assertThat(workloads.release(claim)).isTrue();
                claim = workloads.acquire(claim.key(), owner, 2, liveTtl).claim();
                if (awaitOriginalRetirement) {
                    awaitRetirement(claim.key());
                }
            }
            ClusterRecoveryKey key = new ClusterRecoveryKey("east", id, identity.incarnation());
            ClusterRecoveryEvent event = oldExecution ? new ClusterRecoveryEvent(key, ClusterRecoveryCause.MEMBER_LOSS,
                    claim.executionGeneration(), identity.contentHash(), 1L, profile, profile, 2,
                    DesiredStateFingerprint.of(intent), POSITIONS) : null;
            return new Pipeline(key, claim, event, DesiredStateFingerprint.of(intent));
        }

        private void awaitRetirement(WorkloadClaimKey key) {
            long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
            Document expired = new Document("_id", MongoClusterCapacityStore.claimId(key.clusterId(), key.resourceId()))
                    .append("retiredAuthorizationUntil", new Document("$exists", true))
                    .append("$expr", new Document("$lte", List.of("$retiredAuthorizationUntil", "$$NOW")));
            while (claims.find(expired).first() == null) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("the historical issuer authority did not retire on the server clock");
                }
                try {
                    Thread.sleep(25);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while awaiting historical authority retirement", interrupted);
                }
            }
        }

        private ClusterRecoveryStore.Result enqueue(Pipeline pipeline) {
            return store.enqueue(pipeline.event, ClusterRecoveryFence.enqueue(pipeline.event, WorkloadClaimFence.from(recovery)));
        }

        private void awaitEligibility(ClusterRecoveryKey key) {
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            Document eligible = new Document("_id", ClusterRecoveryDocuments.id(key)).append("$or", List.of(
                    new Document("nextEligibleAt", null),
                    new Document("$expr", new Document("$lte", List.of("$nextEligibleAt", "$$NOW")))));
            while (queue.find(eligible).first() == null) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("recovery backoff did not expire on the server clock");
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while awaiting recovery backoff", interrupted);
                }
            }
        }

        private ClusterRecoveryFence fence(ClusterRecoveryItem item) {
            return new ClusterRecoveryFence(item.event().key(), item.itemRevision(), item.event().intentFingerprint(),
                    item.targetProfile(), item.executionFrontier(), WorkloadClaimFence.from(recovery));
        }

        private ClusterRecoveryPipelineFence pipelineFence(ClusterRecoveryItem item) {
            return new ClusterRecoveryPipelineFence(item.event().key(), item.itemRevision(), item.event().intentFingerprint(),
                    item.targetProfile(), item.successor().pipelineClaim());
        }

        private ClusterRecoveryStore.Result permit(ClusterRecoveryItem item, Map<String, ClusterCapacityDemand> demand, ClusterCapacityLimits limits) {
            return store.acquirePermit(fence(item), demand, limits, PERMIT_TTL, BACKOFF, 1);
        }

        private ClusterRecoveryStore.Result advance(ClusterRecoveryItem item, WorkloadClaim claim) {
            return store.advanceExecution(fence(item), claim, LIVE);
        }

        private ClusterRecoveryStore.Result submitted(ClusterRecoveryItem item) {
            return store.recordSubmission(fence(item), item.successor().pipelineClaim(), "job-" + item.successor().executionGeneration());
        }

        private ClusterRecoveryStore.Result initialized(ClusterRecoveryItem item) {
            ClusterRecoveryStartupReceipt receipt = startupReceipt(item);
            return store.recordStartup(fence(item), item.successor().pipelineClaim(), receipt);
        }

        private ClusterRecoveryStartupReceipt startupReceipt(ClusterRecoveryItem item) {
            java.time.Instant observed = item.updatedAt();
            meta.advanceConsumerReadSeq("capture-crm", SrsConsumerId.of(item.event().key().pipelineId(), "crm").value(), "orders", -1);
            CaptureResumeWitness witness = meta.resumeWitness("crm", "mongo", "capture-crm",
                    SrsConsumerId.of(item.event().key().pipelineId(), "crm").value(), ReadMode.CDC_ONLY, true, List.of("orders"));
            var requested = witness.requestedPosition("capture-crm").orElseThrow();
            assertThat(meta.prepareCaptureResume(item.successor().pipelineClaim(), witness, requested, Set.of("crm"))).isTrue();
            CaptureReadAttempt reader = meta.beginCaptureReadAttempt("capture-crm", captureEpoch, List.of("orders"),
                    CaptureReadAttempt.Kind.RESUME, requested.position().token(), null, WorkloadClaimFence.from(capture)).orElseThrow();
            assertThat(meta.bindCaptureReadAttempt(item.successor().pipelineClaim(), witness, reader, false)).isTrue();
            assertThat(meta.recordCaptureAttachment(item.successor().pipelineClaim(), witness, captureEpoch)).isTrue();
            assertThat(meta.recordCaptureAnchor(reader, requested.position().token())).isTrue();
            assertThat(meta.recordCaptureFirstDelivery(reader)).isTrue();
            var proof = meta.captureStartupProof(item.successor().pipelineClaim(), witness).orElseThrow();
            return new ClusterRecoveryStartupReceipt(
                    item.successor().pipelineClaim(), item.successor().nativeJobId(), observed, Map.of("crm", witness),
                    Map.of("crm", proof.requestedPosition()), Map.of("crm", proof.acceptedPosition()), proof.acceptedAt(), false);
        }

        private void running(String pipeline) {
            var checkpoint = states.read(pipeline).orElseThrow();
            states.compareAndSwap(pipeline, checkpoint.epoch(), StateJson.of(PipelineState.RUNNING), checkpoint.touchTime());
        }

        private void finishHealthy(ClusterRecoveryKey key) {
            ClusterRecoveryItem item = store.read(key).orElseThrow();
            if (item.status().terminal()) {
                return;
            }
            WorkloadClaim claim = workloads.read(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, key.pipelineId())).orElseThrow().claim();
            item = advance(item, claim).item();
            item = submitted(item).item();
            running(key.pipelineId());
            item = initialized(item).item();
            assertThat(store.complete(fence(item)).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
        }

        @Override
        public void close() {
            client.getDatabase(database).drop();
            client.close();
        }
    }
}
