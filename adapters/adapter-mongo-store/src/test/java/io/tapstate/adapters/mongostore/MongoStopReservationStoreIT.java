package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
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

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real conditional stop writes share the checkpoint document and the durable authority document. */
@RequiresDocker
class MongoStopReservationStoreIT {
    private static final String HELD_CLIENT = "held-stop-completion";
    private static final String CLUSTER = "cluster-stop";
    private static final String PIPELINE = "orders";
    private static final Instant T0 = Instant.parse("2026-10-01T00:00:00Z");
    private static final WorkloadClaimKey KEY =
            new WorkloadClaimKey(CLUSTER, WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test
    void reserveKeepsActualAndOriginalIntentThenOnlyExactCompletionStopsIt() {
        try (Fixture fixture = fresh()) {
            assertThat(fixture.state.supportsStopReservations()).isTrue();
            assertThat(new MongoStateStore(fixture.states).supportsStopReservations()).isFalse();
            fixture.seed(PipelineState.RUNNING);
            DesiredState intent = desired(PipelineState.STOPPED, "rev-a");
            fixture.desired.save(intent);
            assertThat(fixture.claims.advanceStandalone(CLUSTER, PIPELINE)).hasValue(1);
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation reservation = existing(before, intent, StopAuthority.standalone(CLUSTER, 1));

            assertThat(fixture.state.reserveStop(before, reservation, T0.plusSeconds(1)))
                    .contains(reservation);
            assertThat(fixture.state.read(PIPELINE).orElseThrow().stateJson())
                    .isEqualTo(StateJson.of(PipelineState.RUNNING));
            assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(1);
            assertThat(fixture.state.readStopReservation(PIPELINE)).contains(reservation);
            try (MongoClient restarted = MongoClients.create(MONGO.getReplicaSetUrl())) {
                MongoStateStore recovered = fixture.stateOn(restarted);
                assertThat(recovered.read(PIPELINE).orElseThrow().stateJson())
                        .isEqualTo(StateJson.of(PipelineState.RUNNING));
                assertThat(recovered.readStopReservation(PIPELINE)).contains(reservation);
            }
            assertThat(fixture.state.compareAndSwap(PIPELINE, 1,
                    StateJson.of(PipelineState.STOPPED), T0.plusSeconds(2)))
                    .isEqualTo(new CasOutcome.Fenced(1));
            assertThat(fixture.state.readStopReservation(PIPELINE)).contains(reservation);
            StopReservation wrongToken = new StopReservation(PIPELINE, "another-work-token",
                    reservation.sourceEpoch(), reservation.reservedEpoch(), intent, reservation.subject());
            assertThat(fixture.state.completeStop(wrongToken, T0.plusSeconds(2))).isEmpty();
            assertThat(fixture.state.readStopReservation(PIPELINE)).contains(reservation);

            CheckpointDoc stopped = fixture.state.completeStop(reservation, T0.plusSeconds(3)).orElseThrow();
            assertThat(stopped.epoch()).isEqualTo(2);
            assertThat(stopped.stateJson()).isEqualTo(StateJson.of(PipelineState.STOPPED));
            assertThat(fixture.state.readStopReservation(PIPELINE)).isEmpty();
            assertThat(fixture.claims.currentGeneration(CLUSTER, PIPELINE)).hasValue(1);
            assertThat(fixture.desired.read(PIPELINE)).contains(intent);
            assertThat(fixture.state.completeStop(reservation, T0.plusSeconds(4))).isEmpty();
        }
    }

    @Test
    void changedDesiredRetiresOnlyTheMarkerAndNeverWritesStopped() {
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.RUNNING);
            DesiredState original = desired(PipelineState.STOPPED, "rev-a");
            fixture.desired.save(original);
            fixture.claims.advanceStandalone(CLUSTER, PIPELINE);
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation reservation = existing(before, original, StopAuthority.standalone(CLUSTER, 1));
            fixture.state.reserveStop(before, reservation, T0.plusSeconds(1)).orElseThrow();

            DesiredState restarted = new DesiredState(PIPELINE, PipelineState.RUNNING, "rev-b",
                    true, "assembly-b", true, before.epoch());
            fixture.desired.save(restarted);
            assertThat(fixture.state.completeStop(reservation, T0.plusSeconds(2))).isEmpty();
            CheckpointDoc retired = fixture.state.retireStop(
                    reservation, restarted, StopAuthority.standalone(CLUSTER, 1), T0.plusSeconds(3)).orElseThrow();

            assertThat(retired.epoch()).isEqualTo(2);
            assertThat(retired.stateJson()).isEqualTo(StateJson.of(PipelineState.RUNNING));
            assertThat(fixture.state.readStopReservation(PIPELINE)).isEmpty();
            assertThat(fixture.desired.read(PIPELINE)).contains(restarted);
        }
    }

    @Test
    void takeoverFencesOldCompletionAndTheNewClaimCanRebindIt() {
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.RUNNING);
            DesiredState intent = desired(PipelineState.STOPPED, "rev-a");
            fixture.desired.save(intent);
            WorkloadClaim first = fixture.claims.acquire(KEY, new WorkloadOwner("node-a", "boot-a"),
                    7, Duration.ofSeconds(30)).claim();
            WorkloadClaim old = fixture.claims.advanceUnderClaim(first, 7).orElseThrow();
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation reservation = existing(before, intent,
                    StopAuthority.claimed(WorkloadClaimFence.from(old)));
            fixture.state.reserveStop(before, reservation, T0.plusSeconds(1)).orElseThrow();

            assertThat(fixture.claims.release(old)).isTrue();
            WorkloadClaim successor = fixture.claims.acquire(KEY,
                    new WorkloadOwner("node-b", "boot-b"), 8, Duration.ofSeconds(30)).claim();
            assertThat(successor.executionGeneration()).isEqualTo(1);
            assertThat(fixture.state.completeStop(reservation, T0.plusSeconds(2))).isEmpty();
            assertThat(fixture.state.readStopReservation(PIPELINE)).contains(reservation);

            StopReservation rebound = fixture.state.rebindStop(reservation,
                    StopAuthority.claimed(WorkloadClaimFence.from(successor)), T0.plusSeconds(3)).orElseThrow();
            assertThat(rebound.sourceEpoch()).isEqualTo(before.epoch());
            assertThat(rebound.originalDesired()).isEqualTo(intent);
            assertThat(rebound.reservedEpoch()).isEqualTo(2);
            assertThat(fixture.state.completeStop(reservation, T0.plusSeconds(4))).isEmpty();
            assertThat(fixture.state.completeStop(rebound, T0.plusSeconds(5)).orElseThrow().epoch()).isEqualTo(3);
            assertThat(fixture.claims.currentGeneration(CLUSTER, PIPELINE)).hasValue(1);
        }
    }

    @Test
    void coldNoJobGuardDoesNotInventGenerationAndBothOriginalAllocatorsCanUseIt() {
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.NEW);
            DesiredState intent = desired(PipelineState.STOPPED, "rev-a");
            fixture.desired.save(intent);
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation cold = new StopReservation(PIPELINE, "no-job", before.epoch(),
                    before.epoch() + 1, intent, new StopReservation.NoJob(CLUSTER, null));
            fixture.state.reserveStop(before, cold, T0.plusSeconds(1)).orElseThrow();

            Document physical = fixture.claimDocument(PIPELINE);
            assertThat(physical).containsEntry("clusterId", CLUSTER)
                    .containsEntry("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                    .containsEntry("resourceId", PIPELINE);
            assertThat(physical).containsKey("fencedAppends")
                    .doesNotContainKeys("executionGeneration", "ownerNodeId", "leaseUntil");
            assertThat(fixture.claims.currentGeneration(CLUSTER, PIPELINE)).isEmpty();
            assertThat(fixture.claims.read(KEY)).isEmpty();

            assertThat(fixture.claims.advanceStandalone(CLUSTER, PIPELINE)).hasValue(1);
            assertThat(fixture.state.completeStop(cold, T0.plusSeconds(2))).isEmpty();
            StopReservation known = fixture.state.rebindStop(cold,
                    StopAuthority.standalone(CLUSTER, 1), T0.plusSeconds(3)).orElseThrow();
            assertThat(known.subject()).isInstanceOf(StopReservation.NoJob.class);
            assertThat(fixture.state.completeStop(known, T0.plusSeconds(4)).orElseThrow().epoch()).isEqualTo(3);
            assertThat(fixture.claims.currentGeneration(CLUSTER, PIPELINE)).hasValue(1);

            String coldClustered = "cold-clustered";
            WorkloadClaimKey key = new WorkloadClaimKey(CLUSTER, WorkloadClaimType.PIPELINE_ACTUATION,
                    coldClustered);
            fixture.state.create(coldClustered, StateJson.of(PipelineState.NEW), T0);
            fixture.desired.save(new DesiredState(coldClustered, PipelineState.STOPPED, "rev-c"));
            CheckpointDoc checkpoint = fixture.state.read(coldClustered).orElseThrow();
            StopReservation noJob = new StopReservation(coldClustered, "no-job-cluster", checkpoint.epoch(),
                    checkpoint.epoch() + 1, fixture.desired.read(coldClustered).orElseThrow(),
                    new StopReservation.NoJob(CLUSTER, null));
            fixture.state.reserveStop(checkpoint, noJob, T0.plusSeconds(1)).orElseThrow();
            WorkloadClaim acquired = fixture.claims.acquire(key,
                    new WorkloadOwner("node-c", "boot-c"), 1, Duration.ofSeconds(30)).claim();
            assertThat(acquired.claimGeneration()).isEqualTo(1);
            assertThat(acquired.executionGeneration()).isZero();
        }
    }

    @Test
    void malformedOrOversizedMarkerIsNeverTreatedAsAnAbsentStop() {
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.NEW);
            fixture.desired.save(desired(PipelineState.STOPPED, "rev-a"));
            fixture.states.updateOne(new Document("_id", PIPELINE),
                    new Document("$set", new Document("stopReservation", new Document("token", "broken"))));
            assertThatThrownBy(() -> fixture.state.readStopReservation(PIPELINE))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(error.args()).containsEntry("field", "stopReservation.sourceEpoch");
                    });
            assertThatThrownBy(() -> fixture.state.read(PIPELINE))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.NEW);
            DesiredState large = desired(PipelineState.STOPPED, "x".repeat(5_000));
            fixture.desired.save(large);
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation proposal = new StopReservation(PIPELINE, "large", before.epoch(),
                    before.epoch() + 1, large, new StopReservation.NoJob(CLUSTER, null));
            assertThatThrownBy(() -> fixture.state.reserveStop(before, proposal, T0.plusSeconds(1)))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE));
            assertThat(fixture.state.readStopReservation(PIPELINE)).isEmpty();
        }
    }

    @Test
    void aDesiredReplacementDuringTheGuardTransactionFencesOldCompletion() throws Exception {
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.RUNNING);
            DesiredState original = desired(PipelineState.STOPPED, "rev-a");
            fixture.desired.save(original);
            fixture.claims.advanceStandalone(CLUSTER, PIPELINE);
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation reservation = existing(before, original, StopAuthority.standalone(CLUSTER, 1));
            fixture.state.reserveStop(before, reservation, T0.plusSeconds(1)).orElseThrow();
            CountDownLatch sent = new CountDownLatch(1);
            try (MongoClient blocked = heldClient("pipeline_desired", sent)) {
                MongoStateStore oldWriter = fixture.stateOn(blocked);
                holdUpdate(fixture, "pipeline_desired", Duration.ofSeconds(4));
                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                    Future<Optional<CheckpointDoc>> completion = pool.submit(
                            () -> oldWriter.completeStop(reservation, T0.plusSeconds(2)));
                    assertThat(sent.await(5, TimeUnit.SECONDS)).as("the old desired guard was sent").isTrue();
                    Thread.sleep(200);
                    assertThat(completion.isDone()).as("the old guard is held before its write").isFalse();
                    DesiredState newer = new DesiredState(PIPELINE, PipelineState.RUNNING,
                            "rev-b", false, "assembly-b", true, before.epoch());
                    fixture.desired.save(newer);
                    assertThat(completion.get(15, TimeUnit.SECONDS)).isEmpty();
                    assertThat(fixture.desired.read(PIPELINE)).contains(newer);
                    assertThat(fixture.state.read(PIPELINE).orElseThrow().stateJson())
                            .isEqualTo(StateJson.of(PipelineState.RUNNING));
                    assertThat(fixture.state.readStopReservation(PIPELINE)).contains(reservation);
                } finally {
                    pool.shutdownNow();
                    releaseUpdate(fixture);
                }
            }
        }
    }

    @Test
    void aClaimTakeoverDuringTheGuardTransactionFencesOldCompletion() throws Exception {
        try (Fixture fixture = fresh()) {
            fixture.seed(PipelineState.RUNNING);
            DesiredState intent = desired(PipelineState.STOPPED, "rev-a");
            fixture.desired.save(intent);
            long leaseStarted = System.nanoTime();
            WorkloadClaim first = fixture.claims.acquire(KEY, new WorkloadOwner("node-a", "boot-a"),
                    7, Duration.ofSeconds(3)).claim();
            WorkloadClaim old = fixture.claims.advanceUnderClaim(first, 7).orElseThrow();
            CheckpointDoc before = fixture.state.read(PIPELINE).orElseThrow();
            StopReservation reservation = existing(before, intent,
                    StopAuthority.claimed(WorkloadClaimFence.from(old)));
            fixture.state.reserveStop(before, reservation, T0.plusSeconds(1)).orElseThrow();
            CountDownLatch sent = new CountDownLatch(1);
            try (MongoClient blocked = heldClient("workload_claims", sent)) {
                MongoStateStore oldWriter = fixture.stateOn(blocked);
                holdUpdate(fixture, "workload_claims", Duration.ofSeconds(6));
                ExecutorService pool = Executors.newSingleThreadExecutor();
                try {
                    Future<Optional<CheckpointDoc>> completion = pool.submit(
                            () -> oldWriter.completeStop(reservation, T0.plusSeconds(2)));
                    assertThat(sent.await(5, TimeUnit.SECONDS)).as("the old live claim guard was sent").isTrue();
                    assertThat(completion.isDone()).as("the old claim guard is held before its write").isFalse();
                    long remaining = Duration.ofMillis(3_500).toNanos() - (System.nanoTime() - leaseStarted);
                    Thread.sleep(Math.max(0L, Duration.ofNanos(remaining).toMillis()));
                    WorkloadClaim successor = fixture.claims.acquire(KEY,
                            new WorkloadOwner("node-b", "boot-b"), 8, Duration.ofSeconds(30)).claim();
                    assertThat(successor.claimGeneration()).isGreaterThan(old.claimGeneration());
                    assertThat(completion.get(15, TimeUnit.SECONDS)).isEmpty();
                    assertThat(fixture.state.readStopReservation(PIPELINE)).contains(reservation);
                    assertThat(fixture.state.read(PIPELINE).orElseThrow().stateJson())
                            .isEqualTo(StateJson.of(PipelineState.RUNNING));
                } finally {
                    pool.shutdownNow();
                    releaseUpdate(fixture);
                }
            }
        }
    }

    private static MongoClient heldClient(String collection, CountDownLatch sent) {
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .applicationName(HELD_CLIENT)
                .addCommandListener(new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if ("update".equals(event.getCommandName())
                                && collection.equals(event.getCommand().getString("update").getValue())) {
                            sent.countDown();
                        }
                    }
                }).build());
    }

    private static void holdUpdate(Fixture fixture, String collection, Duration hold) {
        fixture.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("update"))
                        .append("appName", HELD_CLIENT)
                        .append("namespace", fixture.database.getName() + "." + collection)
                        .append("blockConnection", true).append("blockTimeMS", hold.toMillis())));
    }

    private static void releaseUpdate(Fixture fixture) {
        fixture.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", "off"));
    }

    private static DesiredState desired(PipelineState state, String revision) {
        return new DesiredState(PIPELINE, state, revision, true, "assembly-a", true, 0L);
    }

    private static StopReservation existing(CheckpointDoc before, DesiredState desired, StopAuthority authority) {
        return new StopReservation(PIPELINE, UUID.randomUUID().toString(), before.epoch(), before.epoch() + 1,
                desired, new StopReservation.ExistingJob("inc-a", 1,
                        new StopReservation.JobIdentity(CLUSTER, 42L, "boot-a"), authority));
    }

    private static Fixture fresh() {
        MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        MongoDatabase database = client.getDatabase("stop_reservation_" + UUID.randomUUID().toString().replace("-", ""));
        var states = SystemCollections.PIPELINE_STATE.on(database);
        MongoStateStore state = new MongoStateStore(client, states,
                SystemCollections.PIPELINE_DESIRED.on(database), SystemCollections.WORKLOAD_CLAIMS.on(database));
        return new Fixture(client, database, states, state,
                new MongoDesiredStore(SystemCollections.PIPELINE_DESIRED.on(database)),
                new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database)));
    }

    private record Fixture(MongoClient client, MongoDatabase database, com.mongodb.client.MongoCollection<Document> states,
            MongoStateStore state, MongoDesiredStore desired, MongoWorkloadClaimStore claims) implements AutoCloseable {
        void seed(PipelineState initial) { state.create(PIPELINE, StateJson.of(initial), T0); }
        Document claimDocument(String pipeline) {
            return SystemCollections.WORKLOAD_CLAIMS.on(database)
                    .find(new Document("_id", new Document("clusterId", CLUSTER)
                            .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                            .append("resourceId", pipeline))).first();
        }
        MongoStateStore stateOn(MongoClient anotherClient) {
            MongoDatabase same = anotherClient.getDatabase(database.getName());
            return new MongoStateStore(anotherClient, SystemCollections.PIPELINE_STATE.on(same),
                    SystemCollections.PIPELINE_DESIRED.on(same), SystemCollections.WORKLOAD_CLAIMS.on(same));
        }
        @Override public void close() { client.close(); }
    }
}
