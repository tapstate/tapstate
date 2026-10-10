package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ClusterCapacityReservation;
import io.tapstate.spi.store.ClusterCapacityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryFence;
import io.tapstate.spi.store.ClusterRecoveryIntentFingerprint;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryMutation;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStartupReceipt;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.ExecutionProfile;
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
            ClusterRecoveryItem item = fixture.enqueue(fixture.pipeline("orders", true)).item();
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
        private final MongoClusterProfileStore profiles = new MongoClusterProfileStore(client, profileDocuments, claims, nodeDocuments);
        private final MongoWorkloadClaimStore workloads = new MongoWorkloadClaimStore(claims, profiles);
        private final MongoClusterCapacityStore capacity = new MongoClusterCapacityStore(profiles, workloads, occupancy, claims,
                profileDocuments, memberDocuments, nodeDocuments, desiredDocuments, stateDocuments, artifactDocuments);
        private final MongoClusterRecoveryStore store = new MongoClusterRecoveryStore(queue, capacity);
        private final MongoArtifactStore artifacts = new MongoArtifactStore(client, artifactDocuments);
        private final MongoDesiredStore desired = new MongoDesiredStore(desiredDocuments);
        private final MongoStateStore states = new MongoStateStore(stateDocuments);
        private final WorkloadClaim nodeB;
        private final ClusterExecutionProfile profile;
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
        }

        private MongoCollection<Document> collection(String name) {
            return client.getDatabase(database).getCollection(name);
        }

        private Pipeline pipeline(String id, boolean oldExecution) {
            PipelineResource resource = new PipelineResource(id, null, List.of(SourceRef.bare("crm")),
                    null, null, null, null, Map.of());
            artifacts.create(resource);
            ArtifactIdentity identity = artifacts.identity(id).orElseThrow();
            DesiredState intent = new DesiredState(id, PipelineState.RUNNING, identity.contentHash());
            desired.save(intent);
            states.create(id, StateJson.of(oldExecution ? PipelineState.FAILED : PipelineState.STOPPED), java.time.Instant.now());
            WorkloadClaim claim = workloads.acquire(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, id),
                    nodeB.owner(), oldExecution ? 1 : 2, TTL).claim();
            if (oldExecution) {
                claim = workloads.advanceExecution(claim, 1, Set.of("a", "b", "c")).orElseThrow();
                claims.updateOne(WorkloadClaimDocuments.live(WorkloadClaimFence.from(claim)),
                        new Document("$set", new Document("executionIncarnation", identity.incarnation())
                                .append("executionRevision", identity.contentHash())));
                claim = workloads.recordExecutionFailure(claim, true).orElseThrow();
                claim = workloads.acquire(claim.key(), nodeB.owner(), 2, TTL).claim();
            }
            ClusterRecoveryKey key = new ClusterRecoveryKey("east", id, identity.incarnation());
            ClusterRecoveryEvent event = oldExecution ? new ClusterRecoveryEvent(key, ClusterRecoveryCause.MEMBER_LOSS,
                    claim.executionGeneration(), identity.contentHash(), 1L, profile, profile, 2,
                    ClusterRecoveryIntentFingerprint.of(key.incarnation(), intent), POSITIONS) : null;
            return new Pipeline(key, claim, event, ClusterRecoveryIntentFingerprint.of(key.incarnation(), intent));
        }

        private ClusterRecoveryStore.Result enqueue(Pipeline pipeline) {
            return store.enqueue(pipeline.event, ClusterRecoveryFence.enqueue(pipeline.event, WorkloadClaimFence.from(recovery)));
        }

        private ClusterRecoveryFence fence(ClusterRecoveryItem item) {
            return new ClusterRecoveryFence(item.event().key(), item.itemRevision(), item.event().intentFingerprint(),
                    item.targetProfile(), item.executionFrontier(), WorkloadClaimFence.from(recovery));
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
            java.time.Instant observed = item.updatedAt();
            return store.recordStartup(fence(item), item.successor().pipelineClaim(), new ClusterRecoveryStartupReceipt(
                    item.successor().pipelineClaim(), item.successor().nativeJobId(), observed, POSITIONS, observed, false));
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
