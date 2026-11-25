package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.junit.jupiter.api.RepeatedTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RequiresDocker
class PipelineRenewalCapacityIT {
    @RepeatedTest(2)
    void anEarlierRecoveryNotYetDueMustLeaveProvenIdleCapacityForAnOrdinaryDueLease() throws Exception { exercise(1, 4); }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {3, 4})
    void reservationsRetainEveryEarlierRecoverySlotWithoutConsumingTheWholeLane(int recoveries) throws Exception { exercise(recoveries, 4); }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"1,0", "1,2", "3,2"})
    void unknownOrSmallerDispatchersKeepTheirOriginalCapacityBoundary(int recoveries, int capacity) throws Exception {
        exercise(recoveries, capacity);
    }

    private void exercise(int recoveryCount, int capacity) throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1")) {
            mongo.start();
            var storage = NativeAppStorage.createOnPremWithAdmin(mongo, "recovery_scheduling", "fixture", "fixture-local-password");
            var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    storage.cacheUri(), storage.cacheDatabase(), null);
            try (var configuration = MongoConfigurationDomain.open(new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)), storage.clusterId());
                 var cache = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "recovery-scheduling-" + UUID.randomUUID());
                 var observer = MongoClients.create(mongo.getReplicaSetUrl())) {
                var business = storage.preparation().businessScope();
                var store = (DeadlineWorkloadClaimStore) cache.workloadClaims(business);
                var memberOwner = new WorkloadOwner(storage.clusterId(), "original-boot");
                var nodeKey = new WorkloadClaimKey(storage.clusterId(), WorkloadClaimType.NODE_SESSION, memberOwner.nodeId());
                var node = store.acquire(nodeKey, memberOwner, 0, Duration.ofSeconds(30)).claim();
                var properties = new ClusterProperties();
                var membership = new ClusterMembershipGate(properties);
                membership.bindSingle(node); membership.observeNodeSession(() -> true);
                var local = mock(Member.class);
                when(local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(memberOwner.nodeId());
                when(local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(memberOwner.bootId());
                when(local.localMember()).thenReturn(true);
                assertThat(membership.apply(Set.of(local))).isTrue();
                var offset = new AtomicLong();
                var ownership = PipelineActuationOwnership.scoped(business, memberOwner, membership,
                        new ClusterWorkloadClaims(store, membership), Duration.ofSeconds(30), Duration.ofSeconds(10),
                        () -> System.nanoTime() + offset.get());
                var scopes = new ArrayList<PipelineIntentScope>();
                var ids = new ArrayList<String>();
                for (int i = 0; i < recoveryCount; i++) ids.add("earlier-recovery-" + i);
                ids.add("ordinary-due");
                for (var id : ids) {
                    var pipeline = (PipelineResource) new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: " + id + "\nsource: []\n");
                    configuration.artifacts().create(pipeline);
                    var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), id, 1);
                    var desired = new DesiredState(id, PipelineState.RUNNING, CanonicalHash.of(pipeline), false);
                    configuration.intents().compareAndSet(scope, 0, desired);
                    var issued = ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(scope, WorkloadClaimFence.from(node))).creation();
                    var preparation = cache.admittedPipelinePreparations(business, () -> {});
                    var reserved = preparation.reserveFirst(issued, Duration.ofSeconds(30));
                    configuration.preparations().complete(preparation.seed(reserved, configuration.preparations(), Instant.now()));
                    var permit = ownership.permit(scope);
                    assertThat(permit.granted()).isTrue();
                    assertThat(ownership.beginExecution(scope, WorkloadClaimFence.from(permit.claim())).allowed()).isTrue();
                    scopes.add(scope);
                    TimeUnit.MILLISECONDS.sleep(60);
                }
                var ordinaryKey = new WorkloadClaimKey(business.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scopes.getLast().pipelineId());
                var raw = new MongoWorkloadClaimStore(observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims"));
                var before = raw.read(ordinaryKey).orElseThrow();
                var admin = observer.getDatabase("admin");
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", recoveryCount))
                        .append("data", new Document("failCommands", List.of("findAndModify")).append("errorCode", 112)
                                .append("errorLabels", List.of("TransientTransactionError"))));
                offset.set(TimeUnit.SECONDS.toNanos(11));
                // Prime actual failed renewals before inspecting a dispatcher look with spare capacity.
                var heldField = PipelineActuationOwnership.class.getDeclaredField("held"); heldField.setAccessible(true);
                var held = (Map<?, ?>) heldField.get(ownership);
                for (int i = 0; i < recoveryCount; i++) {
                    Object state = held.get(scopes.get(i).pipelineId());
                    var renewOne = PipelineActuationOwnership.class.getDeclaredMethod("renewScoped", state.getClass(), long.class);
                    renewOne.setAccessible(true);
                    renewOne.invoke(ownership, state, System.nanoTime() + offset.get());
                }
                var afterFirst = raw.read(ordinaryKey).orElseThrow();
                observer.getDatabase(storage.cacheDatabase()).getCollection("local_health_witness")
                        .insertOne(new Document("_id", UUID.randomUUID().toString()));
                long start = System.nanoTime();
                try (var dispatch = new ThreadPoolExecutor(capacity == 0 ? 2 : capacity, capacity == 0 ? 2 : capacity, 1, TimeUnit.SECONDS, new SynchronousQueue<>())) {
                    for (int i = 0; i < 8; i++) {
                        if (capacity == 0) ownership.dispatchDueRenewals(dispatch);
                        else ownership.dispatchDueRenewals(dispatch, capacity);
                        TimeUnit.MILLISECONDS.sleep(40);
                    }
                }
                var afterIdleLooks = raw.read(ordinaryKey).orElseThrow();
                var opsField = MongoCacheDomain.class.getDeclaredField("claimOperations"); opsField.setAccessible(true);
                Object operations = opsField.get(cache);
                var laneField = operations.getClass().getDeclaredField("renewals"); laneField.setAccessible(true);
                var lane = (ThreadPoolExecutor) laneField.get(operations);
                assertThat(lane.getActiveCount()).isZero();
                var callsField = operations.getClass().getDeclaredField("calls"); callsField.setAccessible(true);
                assertThat((Collection<?>) callsField.get(operations)).isEmpty();
                System.out.printf("RECOVERY_SCHEDULING reservations=%d dispatcher_capacity=%d ordinary_due_by_ms=11000 healthy_write=true idle_physical_slots=4 original_live=%s renewed_first=%s renewed_after_idle_looks=%s idle_looks_ms=%d%n",
                        recoveryCount, capacity, afterIdleLooks.leased(), afterFirst.claim().leaseUntil().isAfter(before.claim().leaseUntil()),
                        afterIdleLooks.claim().leaseUntil().isAfter(before.claim().leaseUntil()), TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start));
                if (capacity > 0 && recoveryCount < capacity) assertThat(afterIdleLooks.claim().leaseUntil()).isAfter(before.claim().leaseUntil());
                else assertThat(afterIdleLooks.claim().leaseUntil()).isEqualTo(before.claim().leaseUntil());
                TimeUnit.MILLISECONDS.sleep(1100);
                ownership.renewDue();
                assertThat(raw.read(ordinaryKey).orElseThrow().claim().leaseUntil()).isAfter(before.claim().leaseUntil());
            }
        }
    }
}
