package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.adapters.mongostore.MongoSrsMetaStore;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The boundary between a member's local execution guard and the durable sink acknowledgement. A call held
 * on the store after that guard has admitted it must still be refused when another run becomes current;
 * otherwise the replacement resumes past a position only the superseded run acknowledged.
 */
@RequiresDocker
class DurableSinkAcknowledgementFenceIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final String HELD_CLIENT = "stale-sink-ack";
    private static final String CHAIN = "orders@mysql-1";
    private static final String PIPELINE = "orders-pipeline";
    private static final String TABLE = "orders";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE)
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @ParameterizedTest(name = "snapshot completion: {0}")
    @ValueSource(booleans = {false, true})
    void theSameExecutionKeepsAcknowledgingAfterItsTopologyRevisionChanges(boolean snapshot) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_sink_ack_topology_" + System.nanoTime());
            MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(
                    database.getCollection("workload_claims"));
            MongoSrsMetaStore meta = new MongoSrsMetaStore(
                    client, database.getCollection("srs_meta"), database.getCollection("srs_consumer_offsets"));
            meta.create(CHAIN, null);
            if (snapshot) {
                meta.setCdcStart(CHAIN, PIPELINE, "w0", 1L);
            }

            WorkloadClaimKey key = new WorkloadClaimKey(
                    "cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
            WorkloadOwner owner = new WorkloadOwner("node-a", "boot-a");
            WorkloadClaim claim = claims.acquire(key, owner, 7, Duration.ofSeconds(30)).claim();
            WorkloadClaim run = claims.advanceExecution(claim, 7, Set.of("node-a")).orElseThrow();
            ExecutionFence execution = new ExecutionFence(
                    PIPELINE, run.claimGeneration(), run.executionGeneration());
            AtomicLong now = new AtomicLong();
            Duration window = Duration.ofSeconds(10);

            try (ExecutionAuthorization authorization = new ExecutionAuthorization(
                    "cluster-a", claims, window, now::get)) {
                Map<String, List<String>> plan = snapshot
                        ? Map.of(TABLE, List.of("writer-1"), "items", List.of("writer-1"))
                        : Map.of(TABLE, List.of("writer-1"));
                StoreBackedSinkAckFactory durable = new StoreBackedSinkAckFactory(
                        snapshot ? Map.of(TABLE, CHAIN, "items", CHAIN) : Map.of(TABLE, CHAIN), PIPELINE, meta);
                durable.prepareWriterPlan(plan);
                SinkAckFactory writer = durable.forWriter("writer-1", List.copyOf(plan.keySet()), plan);
                SinkAck ack = FencedSinkAckFactory.heldTo(writer, execution)
                        .resolve(memberWith(meta, authorization));
                ChainPosition snapshotPosition = new ChainPosition(SourceOrder.snapshotRow(1), null);
                ack.advance(TABLE, snapshot ? snapshotPosition : position(1));
                if (snapshot) {
                    assertThat(meta.read(CHAIN).orElseThrow().snapshotCompletedTables(PIPELINE))
                            .containsExactly(TABLE);
                } else {
                    assertThat(ackedBy(meta)).isEqualTo(position(1));
                }

                WorkloadClaim rebound = claims.acquire(key, owner, 8, Duration.ofSeconds(30)).claim();
                assertThat(rebound.claimGeneration()).isEqualTo(run.claimGeneration());
                assertThat(rebound.executionGeneration()).isEqualTo(run.executionGeneration());
                now.addAndGet(window.toNanos());
                assertThat(authorization.require(execution)).isEqualTo(WorkloadClaimFence.from(rebound));

                ack.advance(snapshot ? "items" : TABLE, snapshot ? snapshotPosition : position(2));

                if (snapshot) {
                    assertThat(meta.read(CHAIN).orElseThrow().snapshotCompletedTables(PIPELINE))
                            .containsExactlyInAnyOrder(TABLE, "items");
                    assertThat(meta.read(CHAIN).orElseThrow().consumerOffset(PIPELINE).orElseThrow().sinkAckedByTable())
                            .containsExactlyInAnyOrderEntriesOf(Map.of(
                                    TABLE, new ChainPosition(snapshotPosition.order(), "w0"),
                                    "items", new ChainPosition(snapshotPosition.order(), "w0")));
                    ChainPosition seam = new ChainPosition(snapshotPosition.order(), "w0");
                    assertThat(ackedBy(meta)).as("the jointly confirmed snapshot retains its common source seam").isEqualTo(seam);
                    ack.advance(TABLE, position(1));
                    ack.advance("items", position(2));
                    assertThat(meta.read(CHAIN).orElseThrow().consumerOffset(PIPELINE).orElseThrow().sinkAckedByTable())
                            .containsExactlyInAnyOrderEntriesOf(Map.of(TABLE, position(1), "items", position(2)));
                    assertThat(ackedBy(meta)).as("independent table CDC sequences do not advance the common snapshot seam")
                            .isEqualTo(seam);
                } else {
                    assertThat(ackedBy(meta)).isEqualTo(position(2));
                    assertThat(meta.ringDoneThrough(CHAIN, PIPELINE)).containsEntry(TABLE, 2L);
                    assertThat(meta.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("w2");
                }
            }
        }
    }

    @Test
    void anAcknowledgementAlreadyInFlightCannotAdvanceAfterItsRunIsSuperseded() throws Exception {
        CountDownLatch lateWriteStarted = new CountDownLatch(1);
        AtomicBoolean watchingLateWrite = new AtomicBoolean();
        CommandListener observeLateWrite = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (watchingLateWrite.get() && "update".equals(event.getCommandName())) {
                    lateWriteStarted.countDown();
                }
            }
        };
        String url = REPLICA_SET.getReplicaSetUrl();
        MongoClientSettings staleSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(url))
                .applicationName(HELD_CLIENT)
                .addCommandListener(observeLateWrite)
                .build();
        ExecutorService thread = Executors.newSingleThreadExecutor();

        try (MongoClient currentClient = MongoClients.create(url);
                MongoClient staleClient = MongoClients.create(staleSettings)) {
            String databaseName = "tapstate_sink_ack_fence_" + System.nanoTime();
            var currentDatabase = currentClient.getDatabase(databaseName);
            var staleDatabase = staleClient.getDatabase(databaseName);
            MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(
                    currentDatabase.getCollection("workload_claims"));
            MongoSrsMetaStore currentMeta = new MongoSrsMetaStore(
                    currentClient,
                    currentDatabase.getCollection("srs_meta"),
                    currentDatabase.getCollection("srs_consumer_offsets"));
            MongoSrsMetaStore staleMeta = new MongoSrsMetaStore(
                    staleClient,
                    staleDatabase.getCollection("srs_meta"),
                    staleDatabase.getCollection("srs_consumer_offsets"));
            currentMeta.create(CHAIN, null);

            WorkloadClaimKey key = new WorkloadClaimKey(
                    "cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
            WorkloadClaim firstClaim = claims.acquire(
                    key, new WorkloadOwner("node-a", "boot-a"), 7, Duration.ofSeconds(30)).claim();
            WorkloadClaim firstRun = claims.advanceExecution(firstClaim, 7, Set.of("node-a"))
                    .orElseThrow();
            ExecutionFence staleFence = new ExecutionFence(
                    PIPELINE, firstRun.claimGeneration(), firstRun.executionGeneration());

            boolean failpointEnabled = false;
            try (ExecutionAuthorization authorization = new ExecutionAuthorization(
                    "cluster-a", claims, Duration.ofSeconds(10))) {
                HazelcastInstance member = memberWith(staleMeta, authorization);
                Map<String, List<String>> writerPlan = Map.of(TABLE, List.of("writer-1"));
                StoreBackedSinkAckFactory durable = new StoreBackedSinkAckFactory(
                        Map.of(TABLE, CHAIN), PIPELINE, staleMeta);
                durable.prepareWriterPlan(writerPlan);
                SinkAckFactory writer = durable.forWriter("writer-1", List.of(TABLE), writerPlan);
                SinkAck staleAck = FencedSinkAckFactory.heldTo(writer, staleFence).resolve(member);
                ChainPosition lastCurrentAck = position(1);
                staleAck.advance(TABLE, lastCurrentAck);
                assertThat(ackedBy(currentMeta)).isEqualTo(lastCurrentAck);

                holdNextWrite(currentClient, Duration.ofSeconds(4));
                failpointEnabled = true;
                watchingLateWrite.set(true);
                Future<Long> lateAck = thread.submit(() -> {
                    staleAck.advance(TABLE, position(2));
                    return System.nanoTime();
                });
                assertThat(lateWriteStarted.await(10, TimeUnit.SECONDS))
                        .as("the acknowledgement passed its local guard and reached the store")
                        .isTrue();

                assertThat(claims.release(firstRun)).isTrue();
                WorkloadClaim replacementClaim = claims.acquire(
                        key, new WorkloadOwner("node-b", "boot-b"), 8, Duration.ofSeconds(30)).claim();
                WorkloadClaim replacementRun = claims.advanceExecution(
                        replacementClaim, 8, Set.of("node-b")).orElseThrow();
                long replacedAt = System.nanoTime();
                assertThat(replacementRun.claimGeneration()).isGreaterThan(firstRun.claimGeneration());
                assertThat(replacementRun.executionGeneration()).isGreaterThan(firstRun.executionGeneration());

                long lateAckReturnedAt = lateAck.get(30, TimeUnit.SECONDS);
                assertThat(lateAckReturnedAt)
                        .as("the superseded run's acknowledgement returned only after its replacement existed")
                        .isGreaterThan(replacedAt);
                assertThat(ackedBy(currentMeta))
                        .as("the coordination store must refuse an in-flight acknowledgement from the superseded run")
                        .isEqualTo(lastCurrentAck);
            } finally {
                if (failpointEnabled) {
                    releaseHeldWrites(currentClient);
                }
            }
        } finally {
            thread.shutdownNow();
        }
    }

    private static HazelcastInstance memberWith(
            MongoSrsMetaStore meta, ExecutionAuthorization authorization) {
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        context.put(ExecutionAuthorization.USER_CONTEXT_KEY, authorization);
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(context);
        return member;
    }

    private static ChainPosition position(long seq) {
        return new ChainPosition(new SourceOrder(1, seq), "w" + seq);
    }

    private static ChainPosition ackedBy(MongoSrsMetaStore store) {
        return store.read(CHAIN).orElseThrow().consumerOffset(PIPELINE).orElseThrow().sinkAcked();
    }

    private static void holdNextWrite(MongoClient admin, Duration hold) {
        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("update"))
                        .append("blockConnection", true)
                        .append("blockTimeMS", hold.toMillis())
                        .append("appName", HELD_CLIENT)));
    }

    private static void releaseHeldWrites(MongoClient admin) {
        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", "off"));
    }
}
