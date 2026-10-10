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
import io.tapstate.spi.store.PendingPipelineResume;
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
    void anExplicitResumeMarkerIsAtomicWithItsAcceptedEpochAndOrdinaryCasSupersedesIt() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            assertThat(fixture.states.pendingResume("orders")).contains(resume.pending());
            assertThat(fixture.states.read("orders").orElseThrow().epoch()).isEqualTo(resume.pending().stateEpoch());
            assertThat(fixture.states.compareAndSwap("orders", resume.pending().stateEpoch() - 1, "RUNNING",
                    java.time.Instant.now(), resume.pending())).isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Fenced.class);
            assertThat(fixture.states.pendingResume("orders")).contains(resume.pending());
            assertThat(fixture.states.compareAndSwap("orders", resume.pending().stateEpoch(), "PAUSED", java.time.Instant.now()))
                    .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
            assertThat(fixture.states.pendingResume("orders")).isEmpty();
            assertThat(fixture.capacity.resumeReservation(resume.pending())).isEmpty();
        }
    }

    @Test
    void explicitResumeWaitsForTheOriginalCachedAuthorizationBeforeReservingBudget() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(5));
            assertThat(fixture.workloads.release(resume.original())).isTrue();
            WorkloadClaim current = fixture.workloads.acquire(resume.original().key(), fixture.recovery.owner(), 2, TTL).claim();
            assertThat(fixture.resume(resume.pending(), current).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_EXECUTION);
            assertThat(fixture.states.pendingResume("orders").orElseThrow().reservationId()).isNull();
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
            assertThat(fixture.workloads.read(current.key()).orElseThrow().claim().executionGeneration()).isEqualTo(1);

            fixture.awaitRetirement(current.key());
            var reserved = fixture.resume(resume.pending(), current);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(reserved.reservation().executionGeneration()).isNull();
            PendingPipelineResume linked = fixture.states.pendingResume("orders").orElseThrow();
            assertThat(linked.reservationId()).isEqualTo(reserved.reservation().reservationId());
            assertThat(linked.sameRequestAs(resume.pending())).isTrue();
            Map<String, List<Document>> beforeRead = fixture.storedFacts();
            assertThat(fixture.capacity.resumeReservation(resume.pending())).contains(reserved.reservation());
            assertThat(fixture.storedFacts()).isEqualTo(beforeRead);
            assertThat(fixture.occupancy.countDocuments(new Document("_id", reserved.reservation().reservationId()))).isEqualTo(1);
            assertThat(fixture.occupancy.countDocuments(new Document("pipelineId", "orders")
                    .append("executionGeneration", resume.original().executionGeneration()).append("nativeJobId", "1"))).isEqualTo(1);
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
            assertThat(fixture.states.read("orders").orElseThrow().epoch()).isEqualTo(resume.pending().stateEpoch());
        }
    }

    @Test
    void competingExplicitResumeReservationsLinkOneReceiptAndAllocateOnlyOnce() throws Exception {
        try (Fixture fixture = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim current = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), TTL);
            CountDownLatch start = new CountDownLatch(1);
            var one = workers.submit(() -> { start.await(); return fixture.resume(resume.pending(), current); });
            var two = workers.submit(() -> { start.await(); return fixture.resume(resume.pending(), current); });
            start.countDown();
            var results = List.of(one.get(20, TimeUnit.SECONDS), two.get(20, TimeUnit.SECONDS));
            assertThat(results).extracting(ClusterCapacityStore.Result::outcome).containsExactlyInAnyOrder(
                    ClusterCapacityStore.Outcome.APPLIED, ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(results.getFirst().reservation().reservationId()).isEqualTo(results.getLast().reservation().reservationId());
            ClusterCapacityReservation receipt = fixture.capacity.resumeReservation(resume.pending()).orElseThrow();
            assertThat(fixture.occupancy.countDocuments(new Document("_id", receipt.reservationId()))).isEqualTo(1);
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
            var advanced = fixture.capacity.advanceExecution(receipt, current, LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(advanced.advancedPipelineClaim().executionGeneration()).isEqualTo(2);
            assertThat(fixture.capacity.advanceExecution(receipt, current, LIVE).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_EXECUTION);
            assertThat(fixture.capacity.resumeReservation(resume.pending())).contains(advanced.reservation());
            assertThat(fixture.workloads.read(current.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void aRetiredUnallocatedExplicitReservationRebindsWithoutAnotherReservation() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim first = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), Duration.ofSeconds(3));
            var reserved = fixture.resume(resume.pending(), first);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            WorkloadClaim next = fixture.retireAndAcquire(first, fixture.nodeB.owner(), TTL);
            var adopted = fixture.resume(resume.pending(), next);
            assertThat(adopted.outcome()).isEqualTo(ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(adopted.reservation().reservationId()).isEqualTo(reserved.reservation().reservationId());
            assertThat(adopted.reservation().executionGeneration()).isNull();
            assertThat(adopted.reservation().pipelineClaim()).isEqualTo(WorkloadClaimFence.from(next));
            assertThat(fixture.occupancy.countDocuments(new Document("_id", adopted.reservation().reservationId()))).isEqualTo(1);
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
            assertThat(next.executionGeneration()).isEqualTo(1);
        }
    }

    @Test
    void anAllocatedUnsubmittedExplicitResumeAdoptsTheSameExecutionAfterExactRetirement() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim first = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), Duration.ofSeconds(3));
            var reserved = fixture.resume(resume.pending(), first);
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), first, LIVE);
            assertThat(allocated.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.workloads.release(allocated.advancedPipelineClaim())).isTrue();
            WorkloadClaim next = fixture.workloads.acquire(first.key(), fixture.nodeB.owner(), 2, TTL).claim();
            assertThat(fixture.resume(resume.pending(), next).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_EXECUTION);
            assertThat(fixture.workloads.read(next.key()).orElseThrow().claim().executionClaimGeneration())
                    .isEqualTo(allocated.advancedPipelineClaim().executionClaimGeneration());
            fixture.awaitRetirement(next.key());
            var adopted = fixture.resume(resume.pending(), next);
            assertThat(adopted.outcome()).isEqualTo(ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(adopted.reservation().reservationId()).isEqualTo(allocated.reservation().reservationId());
            assertThat(adopted.reservation().executionGeneration()).isEqualTo(2);
            assertThat(adopted.advancedPipelineClaim().executionGeneration()).isEqualTo(2);
            assertThat(adopted.advancedPipelineClaim().executionClaimGeneration()).isEqualTo(next.claimGeneration());
            assertThat(adopted.advancedPipelineClaim().executionTopologyRevision()).isEqualTo(2);
            assertThat(adopted.advancedPipelineClaim().executionMembers()).isEqualTo(allocated.advancedPipelineClaim().executionMembers());
            var submitted = fixture.capacity.submitted(adopted.reservation(), WorkloadClaimFence.from(adopted.advancedPipelineClaim()), "2");
            assertThat(submitted.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(submitted.reservation().nativeJobId()).isEqualTo("2");
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
            assertThat(fixture.workloads.read(next.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void aSubmittedForeignExplicitReceiptFailsVisiblyAfterRetirementWithoutAllocatingAgain() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim first = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), Duration.ofSeconds(3));
            var reserved = fixture.resume(resume.pending(), first);
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), first, LIVE);
            var submitted = fixture.capacity.submitted(allocated.reservation(), WorkloadClaimFence.from(allocated.advancedPipelineClaim()), "2");
            assertThat(fixture.resume(resume.pending(), allocated.advancedPipelineClaim()).reservation()).isEqualTo(submitted.reservation());
            WorkloadClaim next = fixture.retireAndAcquire(allocated.advancedPipelineClaim(), fixture.nodeB.owner(), TTL);
            assertThat(fixture.resume(resume.pending(), next).outcome()).isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
            assertThat(fixture.capacity.resumeReservation(resume.pending())).contains(submitted.reservation());
            assertThat(fixture.workloads.read(next.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void adoptionRechecksTheSharedBudgetBeforeReauthorizingAnAllocatedResume() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim first = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), Duration.ofSeconds(3));
            var reserved = fixture.resume(resume.pending(), first);
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), first, LIVE);
            assertThat(fixture.workloads.release(allocated.advancedPipelineClaim())).isTrue();
            fixture.awaitRetirement(first.key());
            Pipeline healthy = fixture.pipeline("payments", false);
            Map<String, ClusterCapacityDemand> all = Map.of("a", demand(20), "b", demand(20));
            var other = fixture.capacity.reserve(healthy.claim(), fixture.profile, healthy.key().incarnation(),
                    healthy.intentFingerprint(), all, LIMITS, PERMIT_TTL);
            assertThat(other.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.capacity.advanceExecution(other.reservation(), healthy.claim(), LIVE).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            WorkloadClaim next = fixture.workloads.acquire(first.key(), fixture.nodeB.owner(), 2, TTL).claim();
            var refused = fixture.resume(resume.pending(), next);
            assertThat(refused.outcome()).isEqualTo(ClusterCapacityStore.Outcome.CAPACITY_REFUSED);
            assertThat(refused.refusedNode()).isEqualTo("a");
            assertThat(fixture.capacity.resumeReservation(resume.pending())).contains(allocated.reservation());
            assertThat(fixture.workloads.read(next.key()).orElseThrow().claim().executionClaimGeneration())
                    .isEqualTo(allocated.advancedPipelineClaim().executionClaimGeneration());
            assertThat(next.executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void anAllocatedResumeCannotAdoptAChangedPhysicalCohort() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim first = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), Duration.ofSeconds(3));
            var reserved = fixture.resume(resume.pending(), first);
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), first, LIVE);
            WorkloadClaim next = fixture.retireAndAcquire(allocated.advancedPipelineClaim(), fixture.nodeB.owner(), TTL);
            assertThat(fixture.profiles.markJoined(fixture.nodeB, "uuid-b-new", "b:5701")).isTrue();
            assertThat(fixture.resume(resume.pending(), next).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_EXECUTION);
            assertThat(fixture.workloads.read(next.key()).orElseThrow().claim().executionMembers())
                    .isEqualTo(allocated.advancedPipelineClaim().executionMembers());
            assertThat(next.executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void aChangedCompleteIntentCannotAdvanceTheAcceptedExplicitRequest() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim current = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), TTL);
            var reserved = fixture.resume(resume.pending(), current);
            fixture.desired.save(new DesiredState("orders", PipelineState.RUNNING, current.executionRevision(), false, "another-assembly", true));
            assertThat(fixture.resume(resume.pending(), current).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_INTENT);
            assertThat(fixture.capacity.advanceExecution(reserved.reservation(), current, LIVE).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.STALE_INTENT);
            assertThat(fixture.workloads.read(current.key()).orElseThrow().claim().executionGeneration()).isEqualTo(1);
            assertThat(fixture.capacity.resumeReservation(resume.pending())).contains(reserved.reservation());
        }
    }

    @Test
    void aSupersededResumeCannotAdvanceOrSubmitItsPreviouslyLinkedReceipt() {
        for (boolean allocated : List.of(false, true)) {
            try (Fixture fixture = new Fixture()) {
                Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
                WorkloadClaim current = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), TTL);
                var reserved = fixture.resume(resume.pending(), current);
                ClusterCapacityReservation receipt = reserved.reservation();
                if (allocated) {
                    var advanced = fixture.capacity.advanceExecution(receipt, current, LIVE);
                    receipt = advanced.reservation();
                    current = advanced.advancedPipelineClaim();
                }
                assertThat(fixture.states.compareAndSwap("orders", resume.pending().stateEpoch(), "RUNNING", java.time.Instant.now()))
                        .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
                long frontier = current.executionGeneration();
                assertThat(fixture.resume(resume.pending(), current).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_INTENT);
                assertThat(allocated ? fixture.capacity.submitted(receipt, WorkloadClaimFence.from(current), "2").outcome()
                        : fixture.capacity.advanceExecution(receipt, current, LIVE).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_INTENT);
                assertThat(fixture.capacity.resumeReservation(resume.pending())).isEmpty();
                assertThat(fixture.workloads.read(current.key()).orElseThrow().claim().executionGeneration()).isEqualTo(frontier);
                assertThat(fixture.occupancy.countDocuments(new Document("_id", receipt.reservationId()))).isEqualTo(1);
                assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
            }
        }
    }

    @Test
    void aMalformedResumeOrMissingLinkedReceiptNeverReadsAsAnEmptyBudget() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            Document state = fixture.stateDocuments.find(new Document("_id", "orders")).first();
            fixture.stateDocuments.updateOne(new Document("_id", "orders"), new Document("$set",
                    new Document(PendingPipelineResumeDocuments.FIELD, "invalid")));
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.states.pendingResume("orders"))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            fixture.stateDocuments.replaceOne(new Document("_id", "orders"), state);
            WorkloadClaim current = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), TTL);
            var reserved = fixture.resume(resume.pending(), current);
            fixture.occupancy.deleteOne(new Document("_id", reserved.reservation().reservationId()));
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.capacity.resumeReservation(resume.pending()))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(fixture.resume(resume.pending(), current).outcome()).isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
            assertThat(fixture.workloads.read(current.key()).orElseThrow().claim().executionGeneration()).isEqualTo(1);
        }
    }

    @Test
    void explicitResumeSuppressesOnlyItsAcceptedOriginalExecutionEvent() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim current = fixture.workloads.recordExecutionFailure(resume.original(), true).orElseThrow();
            ClusterRecoveryEvent old = fixture.event(resume.pipeline(), current);
            assertThat(fixture.store.enqueue(old, ClusterRecoveryFence.enqueue(old, WorkloadClaimFence.from(fixture.recovery))).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            assertThat(fixture.queue.countDocuments()).isZero();
            current = fixture.retireAndAcquire(current, fixture.recovery.owner(), TTL);
            var reserved = fixture.resume(resume.pending(), current);
            var advanced = fixture.capacity.advanceExecution(reserved.reservation(), current, LIVE);
            WorkloadClaim failed = fixture.workloads.recordExecutionFailure(advanced.advancedPipelineClaim(), true).orElseThrow();
            ClusterRecoveryEvent later = fixture.event(resume.pipeline(), failed);
            assertThat(fixture.store.enqueue(later, ClusterRecoveryFence.enqueue(later, WorkloadClaimFence.from(fixture.recovery))).outcome())
                    .isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.queue.countDocuments()).isEqualTo(1);
            assertThat(later.originalExecutionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void anOldProfileResumeMarkerDoesNotSuppressARealColdRestartEvent() throws Exception {
        try (Fixture fixture = new Fixture(Duration.ofSeconds(3))) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            ExecutionProfile changed = new ExecutionProfile(1, Map.of("build", "two", "threads", "4"));
            WorkloadOwner newA = new WorkloadOwner("a", "boot-a-next");
            io.tapstate.spi.store.ClusterNodeReservation first;
            long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
            while (true) {
                first = fixture.profiles.reserve("east", newA, URI.create("http://a:8080"), changed, TTL);
                if (first.acquired()) {
                    break;
                }
                assertThat(first.outcome()).isIn(io.tapstate.spi.store.ClusterNodeReservation.Outcome.INCOMPATIBLE,
                        io.tapstate.spi.store.ClusterNodeReservation.Outcome.AUTHORIZATION_HORIZON_ACTIVE);
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("the original profile authority did not expire on the server clock");
                }
                Thread.sleep(25);
            }
            assertThat(first.profile().generation()).isEqualTo(2);
            assertThat(fixture.profiles.markJoined(first.node().registration().nodeSession(), "uuid-a-next", "a:5701")).isTrue();
            WorkloadOwner newB = new WorkloadOwner("b", "boot-b-next");
            var second = fixture.profiles.reserve("east", newB, URI.create("http://b:8080"), changed, TTL);
            assertThat(second.acquired()).isTrue();
            assertThat(fixture.profiles.markJoined(second.node().registration().nodeSession(), "uuid-b-next", "b:5701")).isTrue();
            fixture.memberDocuments.replaceOne(new Document("_id", "east"), new Document("_id", "east")
                    .append("profileGeneration", 2L).append("revision", 3L).append("activeNodeIds", List.of("a", "b")));
            fixture.recovery = fixture.workloads.acquire(recoveryKey(), newA, 3, TTL).claim();
            WorkloadClaim current = fixture.workloads.acquire(resume.original().key(), newB, 3, TTL).claim();
            assertThat(current.executionProfile()).isEqualTo(fixture.profile);
            assertThat(fixture.states.pendingResume("orders")).contains(resume.pending());
            ClusterRecoveryEvent event = new ClusterRecoveryEvent(resume.pipeline().key(), ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                    current.executionGeneration(), current.executionRevision(), current.executionTopologyRevision(), current.executionProfile(),
                    first.profile(), 3, resume.pending().intentFingerprint(), POSITIONS);
            assertThat(fixture.store.enqueue(event, ClusterRecoveryFence.enqueue(event, WorkloadClaimFence.from(fixture.recovery))).outcome())
                    .isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.queue.countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void resumeRetirementProofUsesTheRenewedPromiseAndNeverAdoptsOrAllocates() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim renewed = fixture.workloads.renew(resume.original(), Duration.ofSeconds(5)).orElseThrow();
            assertThat(renewed.leaseUntil()).isAfter(resume.original().leaseUntil());
            assertThat(fixture.workloads.release(renewed)).isTrue();
            WorkloadClaim current = fixture.workloads.acquire(renewed.key(), fixture.recovery.owner(), 2, TTL).claim();
            WorkloadClaimFence former = WorkloadClaimFence.from(resume.original());
            assertThat(fixture.capacity.resumeAuthorityRetired(resume.pending(), current, former)).isFalse();
            long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
            Document pastObserved = new Document("_id", "east").append("$expr", new Document("$gte",
                    List.of("$$NOW", java.util.Date.from(resume.original().leaseUntil()))));
            while (fixture.profileDocuments.find(pastObserved).first() == null) {
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("the originally observed lease deadline did not pass on Mongo time");
                }
                Thread.sleep(25);
            }
            assertThat(fixture.capacity.resumeAuthorityRetired(resume.pending(), current, former)).isFalse();
            fixture.awaitRetirement(current.key());
            assertThat(fixture.capacity.resumeAuthorityRetired(resume.pending(), current, former)).isTrue();
            var unrecognized = new WorkloadClaimFence(former.key(), former.owner(), former.claimGeneration() + 10,
                    former.executionGeneration(), former.topologyRevision(), former.profileGeneration());
            assertThat(fixture.capacity.resumeAuthorityRetired(resume.pending(), current, unrecognized)).isFalse();
            assertThat(fixture.states.pendingResume("orders").orElseThrow().reservationId()).isNull();
            assertThat(fixture.states.read("orders").orElseThrow().epoch()).isEqualTo(resume.pending().stateEpoch());
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
            assertThat(fixture.workloads.read(current.key()).orElseThrow().claim().executionGeneration()).isEqualTo(1);
        }
    }

    @Test
    void aKnownZeroSourceResumeSelectionIsFrozenAndSurvivesAllocatedAdoption() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim current = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), Duration.ofSeconds(3));
            var reserved = fixture.resume(resume.pending(), current);
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), current, LIVE);
            WorkloadClaimFence fence = WorkloadClaimFence.from(allocated.advancedPipelineClaim());
            assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).isEmpty();
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), allocated.reservation(), fence, Set.of()).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            Map<String, List<Document>> beforeRead = fixture.storedFacts();
            assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).contains(Set.of());
            assertThat(fixture.storedFacts()).isEqualTo(beforeRead);
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), allocated.reservation(), fence, Set.of()).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), allocated.reservation(), fence, Set.of("crm")).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
            WorkloadClaim next = fixture.retireAndAcquire(allocated.advancedPipelineClaim(), fixture.nodeB.owner(), TTL);
            var adopted = fixture.resume(resume.pending(), next);
            assertThat(adopted.outcome()).isEqualTo(ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).contains(Set.of());
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), adopted.reservation(),
                    WorkloadClaimFence.from(adopted.advancedPipelineClaim()), Set.of()).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(next.executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void missingSourceSelectionAndMalformedOriginOrSourceTagsNeverBecomeKnownZero() {
        try (Fixture fixture = new Fixture()) {
            Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            WorkloadClaim current = fixture.retireAndAcquire(resume.original(), fixture.recovery.owner(), TTL);
            var reserved = fixture.resume(resume.pending(), current);
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), current, LIVE);
            WorkloadClaimFence fence = WorkloadClaimFence.from(allocated.advancedPipelineClaim());
            assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).isEmpty();
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), allocated.reservation(), fence, Set.of("undeclared")).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
            Document row = fixture.occupancy.find(new Document("_id", allocated.reservation().reservationId())).first();
            for (Object invalid : List.of("invalid", 2.5)) {
                fixture.occupancy.updateOne(new Document("_id", row.get("_id")), new Document("$set",
                        new Document(PendingPipelineResumeDocuments.CAPACITY_EPOCH, invalid)));
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.capacity.resumeSourceRequirements(resume.pending()))
                        .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            }
            fixture.occupancy.replaceOne(new Document("_id", row.get("_id")), row);
            for (Object invalid : List.of(List.of("crm", 7), List.of("crm", "crm"), List.of(""))) {
                fixture.occupancy.updateOne(new Document("_id", row.get("_id")), new Document("$set",
                        new Document("sourceRequirementsRecorded", true).append("requiredSourceIds", invalid)));
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.capacity.resumeSourceRequirements(resume.pending()))
                        .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            }
            fixture.occupancy.replaceOne(new Document("_id", row.get("_id")), row);
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), allocated.reservation(), fence, Set.of("crm")).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).contains(Set.of("crm"));
            fixture.states.compareAndSwap("orders", resume.pending().stateEpoch(), "PAUSED", java.time.Instant.now());
            assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).isEmpty();
            assertThat(fixture.capacity.recordResumeSources(resume.pending(), allocated.reservation(), fence, Set.of("crm")).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.STALE_INTENT);
        }
    }

    @Test
    void originalOrdinarySourceSelectionDistinguishesKnownZeroFromAnUnrecordedRun() {
        for (Set<String> selected : List.of(Set.<String>of(), Set.of("crm"))) {
            try (Fixture fixture = new Fixture()) {
                Resume resume = fixture.resumePipeline("orders", Duration.ofSeconds(3), selected);
                assertThat(fixture.capacity.resumeReservation(resume.pending())).isEmpty();
                Map<String, List<Document>> before = fixture.storedFacts();
                assertThat(fixture.capacity.resumeSourceRequirements(resume.pending())).contains(selected);
                assertThat(fixture.storedFacts()).isEqualTo(before);
                Document original = fixture.occupancy.find(new Document("pipelineId", "orders")).first();
                fixture.occupancy.updateOne(new Document("_id", original.get("_id")), new Document("$set",
                        new Document("requiredSourceIds", List.of("crm", 7))));
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.capacity.resumeSourceRequirements(resume.pending()))
                        .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            }
        }
        try (Fixture fixture = new Fixture()) {
            Resume unknown = fixture.resumePipeline("orders", Duration.ofSeconds(3));
            assertThat(fixture.capacity.resumeSourceRequirements(unknown.pending())).isEmpty();
            ClusterCapacityReservation alreadySubmitted = MongoClusterCapacityStore.reservation(
                    fixture.occupancy.find(new Document("pipelineId", "orders")).first());
            assertThat(fixture.capacity.recordExecutionSources(alreadySubmitted, WorkloadClaimFence.from(unknown.original()), Set.of()).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
            assertThat(fixture.capacity.resumeSourceRequirements(unknown.pending())).isEmpty();
        }
    }

    @Test
    void anEditedTargetResumeRetainsTheOldSourceRevisionUntilTheRealAllocatorWritesTheNewOne() {
        try (Fixture fixture = new Fixture()) {
            Resume initial = fixture.resumePipeline("orders", Duration.ofSeconds(3), Set.of("crm"));
            var running = fixture.states.read("orders").orElseThrow();
            fixture.states.compareAndSwap("orders", running.epoch(), "PAUSED", running.touchTime());
            fixture.artifacts.save(new PipelineResource("orders", new io.tapstate.core.model.Metadata(Map.of(), "Edited target"),
                    List.of(SourceRef.bare("crm")), null, null, null, null, Map.of()));
            ArtifactIdentity target = fixture.artifacts.identity("orders").orElseThrow();
            assertThat(target.incarnation()).isEqualTo(initial.original().executionIncarnation());
            assertThat(target.contentHash()).isNotEqualTo(initial.original().executionRevision());
            DesiredState intent = new DesiredState("orders", PipelineState.RUNNING, target.contentHash(), false, "edited-assembly", true);
            fixture.desired.save(intent);
            var paused = fixture.states.read("orders").orElseThrow();
            PendingPipelineResume pending = new PendingPipelineResume(paused.epoch() + 1, DesiredStateFingerprint.of(intent),
                    initial.original(), "1", "1");
            assertThat(fixture.states.compareAndSwap("orders", paused.epoch(), "RUNNING", paused.touchTime(), pending))
                    .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
            assertThat(fixture.capacity.resumeSourceRequirements(pending)).contains(Set.of("crm"));
            WorkloadClaim current = fixture.retireAndAcquire(initial.original(), fixture.recovery.owner(), TTL);
            var reserved = fixture.resume(pending, current);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(current.executionRevision()).isEqualTo(initial.original().executionRevision());
            var allocated = fixture.capacity.advanceExecution(reserved.reservation(), current, LIVE);
            assertThat(allocated.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(allocated.advancedPipelineClaim().executionRevision()).isEqualTo(target.contentHash());
            assertThat(fixture.states.pendingResume("orders").orElseThrow().originalClaim().executionRevision())
                    .isEqualTo(initial.original().executionRevision());
            assertThat(allocated.advancedPipelineClaim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void activeItemsWithoutTerminalHistoryRemainReadableInSequenceAcrossPages() {
        try (Fixture fixture = new Fixture()) {
            ClusterRecoveryItem first = fixture.enqueue(fixture.pipeline("orders", true)).item();
            ClusterRecoveryItem second = fixture.enqueue(fixture.pipeline("payments", true)).item();
            ClusterRecoveryItem third = fixture.enqueue(fixture.pipeline("shipments", true)).item();
            assertThat(fixture.queue.find().into(new ArrayList<>()))
                    .allSatisfy(row -> assertThat(row).doesNotContainKey("latestTerminal"));
            Map<String, List<Document>> before = fixture.storedFacts();

            org.assertj.core.api.Assertions.assertThatCode(() ->
                    assertThat(fixture.store.list("east", 0, 10)).containsExactly(first, second, third))
                    .doesNotThrowAnyException();
            assertThat(fixture.store.list("east", 0, 1)).containsExactly(first);
            assertThat(fixture.store.list("east", 1, 1)).containsExactly(second);
            assertThat(fixture.store.list("east", 2, 2)).containsExactly(third);
            assertThat(fixture.store.list("east", 3, 1)).isEmpty();
            assertThat(fixture.store.list("west", 0, 10)).isEmpty();
            assertThat(fixture.storedFacts()).isEqualTo(before);
        }
    }

    @Test
    void anExplicitNullTerminalHistoryDoesNotAddAQueueRow() {
        try (Fixture fixture = new Fixture()) {
            ClusterRecoveryItem item = fixture.enqueue(fixture.pipeline("orders", true)).item();
            fixture.queue.updateOne(new Document("_id", ClusterRecoveryDocuments.id(item.event().key())),
                    new Document("$set", new Document("latestTerminal", null)));
            Map<String, List<Document>> before = fixture.storedFacts();

            assertThat(fixture.store.list("east", 0, 10)).containsExactly(item);
            assertThat(fixture.store.list("east", 1, 1)).isEmpty();
            assertThat(fixture.storedFacts()).isEqualTo(before);
        }
    }

    @Test
    void nonDocumentTerminalHistoryDoesNotReplaceAnActiveQueueRow() {
        try (Fixture fixture = new Fixture()) {
            ClusterRecoveryItem item = fixture.enqueue(fixture.pipeline("orders", true)).item();
            for (Object archive : List.of("invalid", 7L, true, List.of(new Document("status", "RECOVERED")))) {
                fixture.queue.updateOne(new Document("_id", ClusterRecoveryDocuments.id(item.event().key())),
                        new Document("$set", new Document("latestTerminal", archive)));
                Map<String, List<Document>> before = fixture.storedFacts();

                org.assertj.core.api.Assertions.assertThatCode(() ->
                        assertThat(fixture.store.list("east", 0, 10)).containsExactly(item))
                        .doesNotThrowAnyException();
                assertThat(fixture.store.list("east", 1, 1)).isEmpty();
                assertThat(fixture.storedFacts()).isEqualTo(before);
            }
        }
    }

    @Test
    void aMalformedTerminalDocumentStillReportsItsCodedReadFailure() {
        try (Fixture fixture = new Fixture()) {
            ClusterRecoveryItem item = fixture.enqueue(fixture.pipeline("orders", true)).item();
            fixture.queue.updateOne(new Document("_id", ClusterRecoveryDocuments.id(item.event().key())),
                    new Document("$set", new Document("latestTerminal", new Document("status", "RECOVERED"))));
            Map<String, List<Document>> before = fixture.storedFacts();

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> fixture.store.list("east", 0, 10))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(fixture.storedFacts()).isEqualTo(before);
        }
    }

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
            assertThat(fixture.store.list("east", 0, 10)).containsExactly(recovered);
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
            Map<String, List<Document>> before = fixture.storedFacts();
            assertThat(fixture.store.list("east", 0, 10)).containsExactly(recovered, queued.item());
            assertThat(fixture.store.list("east", 0, 1)).containsExactly(recovered);
            assertThat(fixture.store.list("east", 1, 1)).containsExactly(queued.item());
            assertThat(fixture.store.list("east", 2, 1)).isEmpty();
            assertThat(fixture.storedFacts()).isEqualTo(before);
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
    void aRetiredSubmittedExecutionAllowsAnOrdinaryStartInANewControllerTenure() {
        try (Fixture fixture = new Fixture()) {
            OrdinaryRestart restart = fixture.ordinaryRestart("restart", true, Duration.ofSeconds(3), true);
            assertThat(restart.controller().claimGeneration()).isGreaterThan(restart.executed().claimGeneration());
            assertThat(restart.controller().executionGeneration()).isEqualTo(restart.executed().executionGeneration());
            assertThat(restart.controller().executionClaimGeneration()).isEqualTo(restart.executed().claimGeneration());
            assertThat(restart.controller().executionMembers()).isEqualTo(restart.executed().executionMembers());
            assertThat(restart.controller().executionProfile()).isEqualTo(restart.executed().executionProfile());
            assertThat(fixture.states.pendingResume("restart")).isEmpty();

            var reserved = fixture.capacity.reserve(restart.controller(), fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, LIMITS, PERMIT_TTL);

            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(reserved.reservation().executionGeneration()).isNull();
            var advanced = fixture.capacity.advanceExecution(reserved.reservation(), restart.controller(), LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(advanced.advancedPipelineClaim().executionGeneration()).isEqualTo(restart.executed().executionGeneration() + 1);
            assertThat(advanced.advancedPipelineClaim().executionClaimGeneration()).isEqualTo(restart.controller().claimGeneration());
            assertThat(fixture.queue.countDocuments()).isZero();
        }
    }

    @Test
    void retiredSubmittedHistorySurvivesBudgetRefusalAndPreAdvanceReentry() {
        try (Fixture fixture = new Fixture()) {
            OrdinaryRestart restart = fixture.ordinaryRestart("restart", true, Duration.ofSeconds(3), true);
            Pipeline busy = fixture.pipeline("busy", false, List.of(SourceRef.bare("crm")), true,
                    fixture.nodeB.owner(), Duration.ofSeconds(5));
            fixture.running("busy");
            var busyReserved = fixture.capacity.reserve(busy.claim(), fixture.profile, busy.key().incarnation(),
                    busy.intentFingerprint(), DEMAND, LIMITS, PERMIT_TTL);
            var busyAdvanced = fixture.capacity.advanceExecution(busyReserved.reservation(), busy.claim(), LIVE);
            assertThat(fixture.capacity.submitted(busyAdvanced.reservation(), WorkloadClaimFence.from(busyAdvanced.advancedPipelineClaim()),
                    "busy-job").outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            ClusterCapacityLimits one = new ClusterCapacityLimits(1, 1, 1, 1, 1, 1);

            assertThat(fixture.capacity.reserve(restart.controller(), fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, one, PERMIT_TTL).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.CAPACITY_REFUSED);
            assertThat(fixture.occupancy.find(new Document("_id", restart.historicalReceipt().reservationId())).first()).isNotNull();
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);

            fixture.desired.save(new DesiredState("busy", PipelineState.STOPPED, busyAdvanced.advancedPipelineClaim().executionRevision()));
            var current = fixture.states.read("busy").orElseThrow();
            fixture.states.compareAndSwap("busy", current.epoch(), "STOPPED", java.time.Instant.now());
            assertThat(fixture.workloads.release(busyAdvanced.advancedPipelineClaim())).isTrue();
            fixture.awaitRetirement(busy.claim().key());
            var reserved = fixture.capacity.reserve(restart.controller(), fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, one, PERMIT_TTL);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
            var retried = fixture.capacity.reserve(restart.controller(), fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, one, PERMIT_TTL);
            assertThat(retried.outcome()).isEqualTo(ClusterCapacityStore.Outcome.ALREADY_RESERVED);
            assertThat(retried.reservation().reservationId()).isEqualTo(reserved.reservation().reservationId());
            assertThat(fixture.occupancy.find(new Document("_id", restart.historicalReceipt().reservationId())).first()).isNotNull();

            var advanced = fixture.capacity.advanceExecution(retried.reservation(), restart.controller(), LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            assertThat(fixture.occupancy.find(new Document("_id", restart.historicalReceipt().reservationId())).first()).isNull();
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
        }
    }

    @Test
    void anUnsubmittedRetiredAllocationCannotProveHistoricalOrdinaryDemand() {
        try (Fixture fixture = new Fixture()) {
            OrdinaryRestart restart = fixture.ordinaryRestart("pending", false, Duration.ofSeconds(3), true);
            assertThat(fixture.capacity.reserve(restart.controller(), fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, LIMITS, PERMIT_TTL).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
        }
    }

    @Test
    void aSubmittedOriginalStillOccupiesBudgetBeforeItsCachedAuthorityRetires() {
        try (Fixture fixture = new Fixture()) {
            OrdinaryRestart restart = fixture.ordinaryRestart("live-horizon", true, Duration.ofSeconds(30), false);
            ClusterCapacityLimits one = new ClusterCapacityLimits(1, 1, 1, 1, 1, 1);
            assertThat(fixture.capacity.reserve(restart.controller(), fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, one, PERMIT_TTL).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.CAPACITY_REFUSED);
            assertThat(fixture.capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEqualTo(DEMAND);
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void aRetiredReceiptCannotQualifyAnotherIncarnationProfileOrExecutionContext() {
        for (String field : List.of("executionIncarnation", "executionProfile", "executionClaimGeneration")) {
            try (Fixture fixture = new Fixture()) {
                OrdinaryRestart restart = fixture.ordinaryRestart("mismatch", true, Duration.ofSeconds(3), true);
                Object incompatible = switch (field) {
                    case "executionIncarnation" -> "another-incarnation";
                    case "executionProfile" -> {
                        ExecutionProfile other = new ExecutionProfile(1, Map.of("build", "other", "threads", "4"));
                        Document original = fixture.claims.find(new Document("_id", MongoClusterCapacityStore.claimId("east", "mismatch")))
                                .first().get("executionProfile", Document.class);
                        yield new Document(original).append("formatVersion", other.formatVersion())
                                .append("attributes", new Document(other.attributes())).append("hash", other.hash());
                    }
                    default -> restart.controller().claimGeneration();
                };
                fixture.claims.updateOne(new Document("_id", MongoClusterCapacityStore.claimId("east", "mismatch")),
                        new Document("$set", new Document(field, incompatible)));
                WorkloadClaim current = fixture.workloads.read(restart.controller().key()).orElseThrow().claim();
                assertThat(fixture.capacity.reserve(current, fixture.profile, restart.pipeline().key().incarnation(),
                        DesiredStateFingerprint.of(restart.accepted()), DEMAND, LIMITS, PERMIT_TTL).outcome()).as(field)
                        .isEqualTo(ClusterCapacityStore.Outcome.UNKNOWN_DEMAND);
            }
        }
    }

    @Test
    void retiredSubmittedHistoryMatchesTheImmutableContextAfterAcquisitionTopologyRefresh() {
        try (Fixture fixture = new Fixture()) {
            OrdinaryRestart restart = fixture.ordinaryRestart("refreshed", true, Duration.ofSeconds(3), true);
            fixture.memberDocuments.updateOne(new Document("_id", "east"), new Document("$set", new Document("revision", 3L)));
            WorkloadClaim refreshed = fixture.workloads.acquire(restart.controller().key(), restart.controller().owner(), 3, TTL).claim();
            assertThat(refreshed.claimGeneration()).isEqualTo(restart.controller().claimGeneration());
            assertThat(refreshed.executionTopologyRevision()).isEqualTo(2L);
            assertThat(refreshed.topologyRevision()).isEqualTo(3L);
            assertThat(fixture.capacity.reserve(refreshed, fixture.profile, restart.pipeline().key().incarnation(),
                    DesiredStateFingerprint.of(restart.accepted()), DEMAND, LIMITS, PERMIT_TTL).outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
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

    @Test
    void aRefreshedTopologyCanRecordAndCompleteTheSameExecutionStartup() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            var originalReceipt = fixture.startupReceipt(item);
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            assertThat(refreshed.claimGeneration()).isEqualTo(allocated.advancedPipelineClaim().claimGeneration());
            assertThat(refreshed.executionGeneration()).isEqualTo(allocated.advancedPipelineClaim().executionGeneration());
            assertThat(refreshed.profileGeneration()).isEqualTo(allocated.advancedPipelineClaim().profileGeneration());
            assertThat(refreshed.owner()).isEqualTo(allocated.advancedPipelineClaim().owner());
            assertThat(refreshed.executionTopologyRevision()).isEqualTo(2);
            assertThat(refreshed.topologyRevision()).isEqualTo(3);
            WorkloadClaimFence current = WorkloadClaimFence.from(refreshed);
            assertThat(fixture.meta.captureStartupProof(current, originalReceipt.preparedWitnesses().get("crm"))).isPresent();
            var received = fixture.store.recordStartup(fixture.fence(item), current, receiptFor(originalReceipt, current));
            assertThat(received.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(received.item().successor().pipelineClaim()).isEqualTo(item.successor().pipelineClaim());
            assertThat(received.item().successor().startupReceipt().pipelineClaim()).isEqualTo(item.successor().pipelineClaim());
            assertThat(fixture.store.complete(fixture.fence(received.item())).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
            assertThat(fixture.workloads.read(refreshed.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void aRefreshedTopologyKeepsTheSameExecutionsQualifiedFailureNote() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            var receipt = fixture.startupReceipt(item);
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            WorkloadClaimFence current = WorkloadClaimFence.from(refreshed);
            var reader = fixture.meta.captureReadState("capture-crm").orElseThrow();
            assertThat(fixture.meta.recordCaptureReadFailure(reader.attempt(), IoError.SRS_PROGRESS_UNPROVEN.code(),
                    Map.of("pipeline", "orders"), "manual-start-required")).isTrue();
            var fact = fixture.meta.captureStartupFailure(current, receipt.preparedWitnesses().get("crm")).orElseThrow();
            var diagnostic = new ClusterRecoveryDiagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                    fact.code(), fact.params(), Map.of("crm", fact.requestedPosition()), fact.disposition());
            var recorded = fixture.store.recordFailureNote(fixture.pipelineFence(item, current), diagnostic,
                    ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, fact);
            assertThat(recorded.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(recorded.item().successor().failureNote().pipelineClaim()).isEqualTo(item.successor().pipelineClaim());
            assertThat(recorded.item().successor().failureNote().diagnostic()).isEqualTo(diagnostic);
            assertThat(recorded.item().permit()).isEqualTo(item.permit());
            assertThat(recorded.item().attempt()).isEqualTo(1);
        }
    }

    @Test
    void completionAlreadyRecordedBeforeACompatibleJoinDoesNotBecomeStale() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            item = fixture.initialized(item).item();
            fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            assertThat(fixture.store.complete(fixture.fence(item)).outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
        }
    }

    @Test
    void aPriorCompatibleRetargetKeepsTheVerifiedOriginalSuccessorCompletable() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            var receipt = fixture.startupReceipt(item);
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            var retargeted = fixture.store.retarget(fixture.fence(item), fixture.profile, 3);
            assertThat(retargeted.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            var current = WorkloadClaimFence.from(refreshed);
            var observed = fixture.store.recordStartup(fixture.fence(retargeted.item()), current, receiptFor(receipt, current));
            assertThat(observed.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            var completed = fixture.store.complete(fixture.fence(observed.item()));
            assertThat(completed.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(completed.item().targetTopologyRevision()).isEqualTo(3);
            assertThat(completed.item().successor().pipelineClaim().topologyRevision()).isEqualTo(2);
            assertThat(completed.item().successor().executionNodeIds()).containsExactlyInAnyOrder("a", "b");
            assertThat(completed.item().successor().startupReceipt().pipelineClaim()).isEqualTo(item.successor().pipelineClaim());
            assertThat(fixture.workloads.read(pipeline.claim.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void aDifferentHolderOrGenerationCannotConsumeTheArchivedStartupOrFailure() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            var receipt = fixture.startupReceipt(item);
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            assertThat(fixture.workloads.release(refreshed)).isTrue();
            WorkloadClaim other = fixture.workloads.acquire(refreshed.key(), fixture.recovery.owner(), 3, TTL).claim();
            WorkloadClaimFence changed = WorkloadClaimFence.from(other);
            assertThat(fixture.store.recordStartup(fixture.fence(item), changed, receiptFor(receipt, changed)).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null), POSITIONS, "Wait for retirement");
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item, changed), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            WorkloadClaim newer = fixture.workloads.advanceExecution(other, 3, Set.of("a", "b", "d")).orElseThrow();
            changed = WorkloadClaimFence.from(newer);
            assertThat(fixture.store.recordStartup(fixture.fence(item), changed, receiptFor(receipt, changed)).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item, changed), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            assertThat(fixture.store.read(item.event().key()).orElseThrow().attempt()).isEqualTo(1);
        }
    }

    @Test
    void aTopologyRefreshCannotRetireCapacityWhileTheSameAuthorityRemainsLive() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true, List.of(SourceRef.bare("crm")), true,
                    fixture.nodeB.owner(), Duration.ofSeconds(8));
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            ClusterCapacityReservation reservation = MongoClusterCapacityStore.reservation(
                    fixture.occupancy.find(new Document("_id", item.permit().reservationId())).first());
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), Duration.ofSeconds(20));
            fixture.awaitRetirement(refreshed.key());
            assertThat(fixture.workloads.read(refreshed.key()).orElseThrow().leased()).isTrue();
            assertThat(fixture.capacity.release(reservation).outcome()).isEqualTo(ClusterCapacityStore.Outcome.STALE_CLAIM);
            assertThat(fixture.occupancy.countDocuments()).isEqualTo(1);
            assertThat(fixture.capacity.occupied("east")).isEqualTo(DEMAND);
        }
    }

    @Test
    void aRefreshedTopologyCanSubmitTheAlreadyAllocatedExecution() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = allocated.item();
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            var submitted = fixture.store.recordSubmission(fixture.fence(item), WorkloadClaimFence.from(refreshed), "job-2");
            assertThat(submitted.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(submitted.item().successor().pipelineClaim()).isEqualTo(item.successor().pipelineClaim());
            var reservation = MongoClusterCapacityStore.reservation(
                    fixture.occupancy.find(new Document("_id", item.permit().reservationId())).first());
            assertThat(reservation.pipelineClaim()).isEqualTo(item.successor().pipelineClaim());
            assertThat(reservation.nativeJobId()).isEqualTo("job-2");
            assertThat(fixture.workloads.read(refreshed.key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void aFailureObservedBeforeACompatibleJoinCanBeNotedWithTheCurrentAuthority() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = fixture.submitted(allocated.item()).item();
            fixture.running("orders");
            var receipt = fixture.startupReceipt(item);
            var reader = fixture.meta.captureReadState("capture-crm").orElseThrow();
            assertThat(fixture.meta.recordCaptureReadFailure(reader.attempt(), IoError.SRS_PROGRESS_UNPROVEN.code(),
                    Map.of("pipeline", "orders"), "manual-start-required")).isTrue();
            var cached = fixture.meta.captureStartupFailure(item.successor().pipelineClaim(),
                    receipt.preparedWitnesses().get("crm")).orElseThrow();
            var diagnostic = new ClusterRecoveryDiagnostic(ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                    cached.code(), cached.params(), Map.of("crm", cached.requestedPosition()), cached.disposition());
            WorkloadClaim refreshed = fixture.compatibleAddition(allocated.advancedPipelineClaim(), TTL);
            var recorded = fixture.store.recordFailureNote(fixture.pipelineFence(item, WorkloadClaimFence.from(refreshed)),
                    diagnostic, ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION, cached);
            assertThat(recorded.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(recorded.item().successor().failureNote().pipelineClaim()).isEqualTo(cached.pipelineClaim());
            assertThat(recorded.item().successor().failureNote().diagnostic()).isEqualTo(diagnostic);
            assertThat(recorded.item().attempt()).isEqualTo(1);
        }
    }

    @Test
    void aFreshPermitUsesItsPreAllocationRefusalInsteadOfTheHistoricalFailureNote() {
        try (Fixture fixture = new Fixture()) {
            var retry = fixture.permitAfterNotedFailure();
            ClusterRecoveryItem item = retry.item();
            assertThat(item.successor().failureNote()).isNotNull();
            assertThat(item.permit().transferredExecutionGeneration()).isZero();
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item, WorkloadClaimFence.from(retry.claim())),
                    item.successor().failureNote().diagnostic(), ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null)
                    .outcome()).isNotEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.recordStartup(fixture.fence(item), WorkloadClaimFence.from(retry.claim()),
                    receiptFor(retry.receipt(), WorkloadClaimFence.from(retry.claim()))).outcome())
                    .isNotEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.store.complete(fixture.fence(item)).outcome()).isNotEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(fixture.workloads.release(retry.claim())).isTrue();
            fixture.awaitRetirement(retry.claim().key());
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(io.tapstate.core.lifecycle.LifecycleError.PIPELINE_NOT_RUNNABLE,
                            Map.of("pipeline", "orders"), null), POSITIONS, "Correct the current compilation refusal");
            var refused = fixture.store.fail(fixture.fence(item), WorkloadClaimFence.from(retry.claim()), diagnostic,
                    ClusterRecoveryStore.FailureStage.BEFORE_EXECUTION_ADVANCE, Duration.ofMillis(1));
            assertThat(refused.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(refused.item().diagnostic()).isEqualTo(diagnostic);
            assertThat(refused.item().attempt()).isEqualTo(2);
            assertThat(refused.item().status()).isEqualTo(ClusterRecoveryStatus.RETRY_BACKOFF);
            assertThat(fixture.store.fail(fixture.fence(item), WorkloadClaimFence.from(retry.claim()), diagnostic,
                    ClusterRecoveryStore.FailureStage.BEFORE_EXECUTION_ADVANCE, Duration.ofMillis(1)).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_ITEM);
            assertThat(fixture.store.read(item.event().key()).orElseThrow().attempt()).isEqualTo(2);
        }
    }

    @Test
    void anExpiredFreshPermitDoesNotConsumeTheHistoricalExecutionFailureAgain() {
        try (Fixture fixture = new Fixture()) {
            var retry = fixture.permitAfterNotedFailure();
            assertThat(fixture.workloads.release(retry.claim())).isTrue();
            fixture.awaitRetirement(retry.claim().key());
            var expired = fixture.store.releaseExpiredPermit(fixture.fence(retry.item()));
            assertThat(expired.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(expired.item().status()).isEqualTo(ClusterRecoveryStatus.WAITING_PERMIT);
            assertThat(expired.item().attempt()).isEqualTo(1);
            assertThat(expired.item().permit()).isNull();
            assertThat(expired.item().successor().failureNote()).isEqualTo(retry.item().successor().failureNote());
            assertThat(fixture.workloads.read(retry.claim().key()).orElseThrow().claim().executionGeneration()).isEqualTo(2);
        }
    }

    @Test
    void anUnsubmittedAllocationCanReportFailureAndRepairOnlyItsExactOlderMarker() {
        try (Fixture fixture = new Fixture()) {
            Pipeline pipeline = fixture.pipeline("orders", true);
            ClusterRecoveryItem item = fixture.permit(fixture.enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = fixture.advance(item, pipeline.claim);
            item = allocated.item();
            assertThat(fixture.workloads.recordExecutionFailure(allocated.advancedPipelineClaim(), false)).isPresent();
            assertThat(fixture.queue.updateOne(new Document("_id", ClusterRecoveryDocuments.id(item.event().key())),
                    new Document("$set", new Document("permit.transferredExecutionGeneration", 0L))).getMatchedCount()).isEqualTo(1);
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null),
                    POSITIONS, "Wait for the allocated execution authority to retire");
            var noted = fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null);
            assertThat(noted.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(noted.item().permit().transferredExecutionGeneration()).isEqualTo(2);
            assertThat(noted.item().successor().nativeJobId()).isNull();
            assertThat(fixture.occupancy.find(new Document("_id", item.permit().reservationId())).first().get("nativeJobId")).isNull();
            item = noted.item();
            fixture.queue.updateOne(new Document("_id", ClusterRecoveryDocuments.id(item.event().key())),
                    new Document("$set", new Document("permit.transferredExecutionGeneration", 0L)));
            fixture.occupancy.updateOne(new Document("_id", item.permit().reservationId()),
                    new Document("$set", new Document("executionGeneration", null)));
            assertThat(fixture.store.recordFailureNote(fixture.pipelineFence(item), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).outcome())
                    .isEqualTo(ClusterRecoveryMutation.STALE_EXECUTION);
            assertThat(fixture.store.read(item.event().key()).orElseThrow().attempt()).isEqualTo(1);
        }
    }

    private static ClusterRecoveryStartupReceipt receiptFor(ClusterRecoveryStartupReceipt receipt, WorkloadClaimFence pipeline) {
        return new ClusterRecoveryStartupReceipt(pipeline, receipt.nativeJobId(), receipt.nativeInitializedAt(),
                receipt.preparedWitnesses(), receipt.requestedPositions(), receipt.acceptedPositions(),
                receipt.positionsAcceptedAt(), receipt.executionCompleted());
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
    private record RetryPermit(ClusterRecoveryItem item, WorkloadClaim claim, ClusterRecoveryStartupReceipt receipt) {}
    private record Resume(Pipeline pipeline, WorkloadClaim original, PendingPipelineResume pending) {}

    private record OrdinaryRestart(Pipeline pipeline, WorkloadClaim executed, WorkloadClaim controller,
            ClusterCapacityReservation historicalReceipt, DesiredState accepted) {}

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
            this(TTL);
        }

        private Fixture(Duration sessionTtl) {
            ExecutionProfile proposed = new ExecutionProfile(1, Map.of("build", "one", "threads", "4"));
            List<WorkloadClaim> members = new ArrayList<>();
            for (String node : List.of("a", "b", "c")) {
                WorkloadClaim claim = profiles.reserve("east", new WorkloadOwner(node, "boot-" + node),
                        URI.create("http://" + node + ":8080"), proposed, sessionTtl).node().registration().nodeSession();
                profiles.markJoined(claim, "uuid-" + node, node + ":5701");
                members.add(claim);
            }
            nodeB = members.get(1);
            profile = profiles.profile("east").orElseThrow();
            memberDocuments.insertOne(new Document("_id", "east").append("profileGeneration", profile.generation())
                    .append("revision", 2L).append("activeNodeIds", List.of("a", "b", "c")));
            workloads.release(members.get(2));
            recovery = workloads.acquire(recoveryKey(), members.getFirst().owner(), 2, sessionTtl).claim();
            capture = workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "capture-crm"),
                    members.getFirst().owner(), 2, sessionTtl).claim();
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

        private Map<String, List<Document>> storedFacts() {
            return Map.of("queue", queue.find().into(new ArrayList<>()),
                    "profiles", profileDocuments.find().into(new ArrayList<>()),
                    "claims", claims.find().into(new ArrayList<>()),
                    "capacity", occupancy.find().into(new ArrayList<>()));
        }

        private Resume resumePipeline(String id, Duration ttl) {
            return resumePipeline(id, ttl, null);
        }

        private Resume resumePipeline(String id, Duration ttl, Set<String> originalSources) {
            Pipeline pipeline = pipeline(id, false, List.of(SourceRef.bare("crm")), true, nodeB.owner(), ttl);
            var reserved = capacity.reserve(pipeline.claim, profile, pipeline.key.incarnation(), pipeline.intentFingerprint,
                    DEMAND, LIMITS, PERMIT_TTL);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            var advanced = capacity.advanceExecution(reserved.reservation(), pipeline.claim, LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            if (originalSources != null) {
                assertThat(capacity.recordExecutionSources(advanced.reservation(), WorkloadClaimFence.from(advanced.advancedPipelineClaim()),
                        originalSources).outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            }
            assertThat(capacity.submitted(advanced.reservation(), WorkloadClaimFence.from(advanced.advancedPipelineClaim()), "1").outcome())
                    .isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            var checkpoint = states.read(id).orElseThrow();
            var paused = (io.tapstate.core.lifecycle.CasOutcome.Applied) states.compareAndSwap(id, checkpoint.epoch(), "PAUSED", checkpoint.touchTime());
            var pending = new PendingPipelineResume(paused.next().epoch() + 1, pipeline.intentFingerprint,
                    advanced.advancedPipelineClaim(), "1", "1");
            assertThat(states.compareAndSwap(id, paused.next().epoch(), "RUNNING", checkpoint.touchTime(), pending))
                    .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
            return new Resume(pipeline, advanced.advancedPipelineClaim(), pending);
        }

        private WorkloadClaim retireAndAcquire(WorkloadClaim original, WorkloadOwner owner, Duration ttl) {
            assertThat(workloads.release(original)).isTrue();
            awaitRetirement(original.key());
            return workloads.acquire(original.key(), owner, original.topologyRevision(), ttl).claim();
        }

        private OrdinaryRestart ordinaryRestart(String id, boolean submitted, Duration originalTtl, boolean awaitRetired) {
            Pipeline pipeline = pipeline(id, false, List.of(SourceRef.bare("crm")), true, nodeB.owner(), originalTtl);
            running(id);
            var reserved = capacity.reserve(pipeline.claim(), profile, pipeline.key().incarnation(), pipeline.intentFingerprint(),
                    DEMAND, LIMITS, PERMIT_TTL);
            assertThat(reserved.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            var advanced = capacity.advanceExecution(reserved.reservation(), pipeline.claim(), LIVE);
            assertThat(advanced.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
            WorkloadClaim executed = advanced.advancedPipelineClaim();
            ClusterCapacityReservation receipt = advanced.reservation();
            if (submitted) {
                var recorded = capacity.submitted(receipt, WorkloadClaimFence.from(executed), "ordinary-original-job");
                assertThat(recorded.outcome()).isEqualTo(ClusterCapacityStore.Outcome.APPLIED);
                receipt = recorded.reservation();
            }
            desired.save(new DesiredState(id, PipelineState.STOPPED, executed.executionRevision()));
            var checkpoint = states.read(id).orElseThrow();
            var stopped = (io.tapstate.core.lifecycle.CasOutcome.Applied) states.compareAndSwap(id, checkpoint.epoch(),
                    StateJson.of(PipelineState.STOPPED), java.time.Instant.now());
            assertThat(workloads.release(executed)).isTrue();
            WorkloadClaim controller = workloads.acquire(executed.key(), recovery.owner(), executed.topologyRevision(), TTL).claim();
            if (awaitRetired) {
                awaitRetirement(controller.key());
                assertThat(capacity.readOccupied("east").orElseThrow().occupiedByNode()).isEmpty();
            }
            DesiredState accepted = new DesiredState(id, PipelineState.RUNNING, executed.executionRevision(),
                    false, null, true, stopped.next().epoch());
            desired.save(accepted);
            assertThat(states.compareAndSwap(id, stopped.next().epoch(), StateJson.of(PipelineState.RUNNING), java.time.Instant.now()))
                    .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
            assertThat(states.pendingResume(id)).isEmpty();
            return new OrdinaryRestart(pipeline, executed, controller, receipt, accepted);
        }

        private ClusterCapacityStore.Result resume(PendingPipelineResume pending, WorkloadClaim current) {
            return capacity.reserveResume(pending, current, profile, pending.originalClaim().executionIncarnation(),
                    pending.intentFingerprint(), DEMAND, LIMITS, PERMIT_TTL);
        }

        private ClusterRecoveryEvent event(Pipeline pipeline, WorkloadClaim claim) {
            return new ClusterRecoveryEvent(pipeline.key, ClusterRecoveryCause.MEMBER_LOSS, claim.executionGeneration(),
                    claim.executionRevision(), claim.executionTopologyRevision(), claim.executionProfile(), profile, 2,
                    pipeline.intentFingerprint, POSITIONS);
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
            return pipelineFence(item, item.successor().pipelineClaim());
        }

        private ClusterRecoveryPipelineFence pipelineFence(ClusterRecoveryItem item, WorkloadClaimFence pipeline) {
            return new ClusterRecoveryPipelineFence(item.event().key(), item.itemRevision(), item.event().intentFingerprint(),
                    item.targetProfile(), pipeline);
        }

        private WorkloadClaim compatibleAddition(WorkloadClaim pipeline, Duration ttl) {
            WorkloadClaim newMember = profiles.reserve("east", new WorkloadOwner("d", "boot-d"),
                    URI.create("http://d:8080"), profile.profile(), TTL).node().registration().nodeSession();
            assertThat(profiles.markJoined(newMember, "uuid-d", "d:5701")).isTrue();
            var committed = memberDocuments.updateOne(new Document("_id", "east").append("revision", 2L),
                    new Document("$set", new Document("revision", 3L).append("activeNodeIds", List.of("a", "b", "c", "d"))));
            assertThat(committed.getModifiedCount()).isEqualTo(1);
            return workloads.acquire(pipeline.key(), pipeline.owner(), 3, ttl).claim();
        }

        private RetryPermit permitAfterNotedFailure() {
            Pipeline pipeline = pipeline("orders", true, List.of(SourceRef.bare("crm")), true, nodeB.owner(), Duration.ofSeconds(8));
            ClusterRecoveryItem item = permit(enqueue(pipeline).item(), DEMAND, LIMITS).item();
            var allocated = advance(item, pipeline.claim());
            item = submitted(allocated.item()).item();
            running("orders");
            ClusterRecoveryStartupReceipt receipt = startupReceipt(item);
            assertThat(workloads.recordExecutionFailure(allocated.advancedPipelineClaim(), false)).isPresent();
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null),
                    POSITIONS, "Wait for the first execution authority to retire");
            item = store.recordFailureNote(pipelineFence(item), diagnostic, ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, null).item();
            var checkpoint = states.read("orders").orElseThrow();
            assertThat(states.compareAndSwap("orders", checkpoint.epoch(), StateJson.of(PipelineState.FAILED), checkpoint.touchTime()))
                    .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
            assertThat(workloads.release(allocated.advancedPipelineClaim())).isTrue();
            awaitRetirement(allocated.advancedPipelineClaim().key());
            var failed = store.fail(fence(item), item.successor().pipelineClaim(), diagnostic,
                    ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, Duration.ofMillis(1));
            assertThat(failed.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            awaitEligibility(pipeline.key());
            WorkloadClaim next = workloads.acquire(pipeline.claim().key(), nodeB.owner(), 2, Duration.ofSeconds(3)).claim();
            var reserved = permit(failed.item(), DEMAND, LIMITS);
            assertThat(reserved.outcome()).isEqualTo(ClusterRecoveryMutation.APPLIED);
            assertThat(reserved.item().attempt()).isEqualTo(1);
            return new RetryPermit(reserved.item(), next, receipt);
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
