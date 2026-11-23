package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.jet.core.DAG;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.engine.*;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.bson.Document;
import org.junit.jupiter.api.RepeatedTest;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@RequiresDocker
class CapturePreparationLeaseProofIT {
    @RepeatedTest(2)
    void liveOriginalLeaseMustReachCapturePreparationWhenItsIndependentReadCannotGetAConnection() throws Exception {
        exercise("blocked");
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"expired", "owner", "topology", "execution", "after-capture-owner"})
    void actualAuthorityLossStillRefusesPreparationOrItsNextDurableBoundary(String scenario) throws Exception { exercise(scenario); }

    private void exercise(String scenario) throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1")) {
            mongo.start();
            var storage = NativeAppStorage.createOnPremWithAdmin(mongo, "capture_guard", "fixture", "fixture-local-password");
            String uri = storage.cacheUri() + (storage.cacheUri().contains("?") ? "&" : "?") + "maxPoolSize=1";
            var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri, storage.cacheDatabase(), null);
            String process = "r26-" + UUID.randomUUID();
            try (var configuration = MongoConfigurationDomain.open(new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)), storage.clusterId());
                 var cache = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), process);
                 var observer = MongoClients.create(mongo.getReplicaSetUrl());
                 var holder = Executors.newSingleThreadExecutor()) {
                var business = storage.preparation().businessScope();
                var store = (DeadlineWorkloadClaimStore) cache.workloadClaims(business);
                var memberOwner = new WorkloadOwner(storage.clusterId(), "original-boot");
                var nodeKey = new WorkloadClaimKey(storage.clusterId(), WorkloadClaimType.NODE_SESSION, memberOwner.nodeId());
                var node = store.acquire(nodeKey, memberOwner, 0, Duration.ofSeconds(30)).claim();
                var properties = new ClusterProperties();
                assertThat(properties.explicitSingle()).isFalse();
                var membership = new ClusterMembershipGate(properties); membership.bindSingle(node); membership.observeNodeSession(() -> true);
                var local = mock(Member.class); when(local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(memberOwner.nodeId());
                when(local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(memberOwner.bootId()); when(local.localMember()).thenReturn(true);
                assertThat(membership.apply(Set.of(local))).isTrue(); assertThat(membership.businessEligible()).isTrue();
                var pipeline = (PipelineResource) new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: capture-proof\nsource: []\n");
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), pipeline.id(), 1);
                var desired = new DesiredState(pipeline.id(), PipelineState.RUNNING, CanonicalHash.of(pipeline), false);
                configuration.intents().compareAndSet(scope, 0, desired);
                var issued = ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(scope, WorkloadClaimFence.from(node))).creation();
                var preparation = cache.admittedPipelinePreparations(business, () -> {});
                var reserved = preparation.reserveFirst(issued, Duration.ofSeconds(30));
                configuration.preparations().complete(preparation.seed(reserved, configuration.preparations(), Instant.now()));
                var offset = new AtomicLong();
                var ownership = PipelineActuationOwnership.scoped(business, memberOwner, membership,
                        new ClusterWorkloadClaims(store, membership), Duration.ofSeconds(30), Duration.ofSeconds(10),
                        () -> System.nanoTime() + offset.get());
                var permit = ownership.permit(scope); assertThat(permit.granted()).isTrue();
                var pass = new PipelineConvergencePass(new PipelineIntentSnapshot(scope, 1, desired), WorkloadClaimFence.from(permit.claim()), cache, () -> {});
                var engine = mock(Engine.class); when(engine.captureRun(pipeline.id())).thenReturn(Optional.empty());
                var dags = mock(DagSource.class); var captures = mock(PipelineCaptureCoordinator.class); var fixed = mock(PipelineCaptureCoordinator.class);
                when(captures.forRun(any(), any(), any())).thenReturn(fixed);
                var opened = new AtomicBoolean(); when(fixed.prepareCaptureStart(anyString(), any(), any())).thenReturn(() -> {
                    opened.set(true);
                    if (scenario.equals("after-capture-owner")) {
                        var raw = new MongoWorkloadClaimStore(observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims"));
                        var current = raw.read(pass.claim().key()).orElseThrow().claim();
                        assertThat(raw.release(current)).isTrue();
                        assertThat(raw.acquire(current.key(), new WorkloadOwner("replacement", "replacement-boot"), current.topologyRevision(), Duration.ofSeconds(30)).acquired()).isTrue();
                        return;
                    }
                    throw new CaptureReached();
                });
                when(fixed.captureBindings()).thenAnswer(ignored -> new PipelineCaptureBindings(new ExecutionFence(scope, pass.claim()), Map.of(), false));
                var plan = OperatorCoordinatePlan.compile(scope, pipeline, storage.cacheDatabase(), step -> null, Map.of(), NestSettings.defaults());
                var prepared = new DagSource.StartPreparation(DagSource.NestCapacity.none(), Set.of(), Optional.of(mock(ArtifactStore.class)),
                        () -> new DagSource.PlannedTopology(List.of(memberOwner.nodeId()), fence -> new DagSource.PlannedDag(new DAG(), ExecutionShape.totalOne(), List.of(memberOwner.nodeId()), Map.of(), Map.of())),
                        Map.of(), Optional.of(SourceModelSnapshot.capture(pipeline.id(), List.of(), mock(SchemaStore.class))), Optional.of(plan));
                when(dags.prepareStart(any(), anyString(), any())).thenReturn(prepared);
                var mainField = MongoCacheDomain.class.getDeclaredField("client"); mainField.setAccessible(true);
                var main = (MongoClient) mainField.get(cache);
                var blocked = new AtomicReference<Future<?>>(); var renewed = new AtomicReference<WorkloadClaim>();
                var instance = mock(com.hazelcast.core.HazelcastInstance.class); var cluster = mock(com.hazelcast.cluster.Cluster.class);
                when(instance.getCluster()).thenReturn(cluster); when(cluster.getMembers()).thenReturn(Set.of(local)); when(cluster.getLocalMember()).thenReturn(local);
                var records = new java.util.concurrent.CopyOnWriteArrayList<Map<?, ?>>();
                var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("io.tapstate.diagnostics.pipeline");
                var originalLevel = logger.getLevel();
                logger.setLevel(ch.qos.logback.classic.Level.DEBUG);
                var appender = new ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
                    @Override protected void append(ch.qos.logback.classic.spi.ILoggingEvent event) {
                        if (event.getFormattedMessage().contains("mongo_claim_io_diag")) records.add((Map<?, ?>) io.tapstate.core.common.JsonReader.parse(event.getFormattedMessage()));
                    }
                }; appender.setContext(logger.getLoggerContext()); appender.start(); logger.addAppender(appender);
                var ports = new ScopedEngineLifecycleActuator.Ports((manifest, capacities, members, guard) -> {
                    guard.run();
                    offset.addAndGet(Duration.ofSeconds(10).toNanos()); ownership.renewDue();
                    var key = new WorkloadClaimKey(business.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline.id());
                    renewed.set(new MongoWorkloadClaimStore(observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims")).read(key).orElseThrow().claim());
                    assertThat(renewed.get().executionGeneration()).isEqualTo(1);
                    var original = new ExecutionFence(scope, pass.claim());
                    assertThat(ownership.hasLocalWriterLease(original)).isTrue();
                    var raw = new MongoWorkloadClaimStore(observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims"));
                    if (scenario.equals("expired")) {
                        offset.addAndGet(Duration.ofSeconds(31).toNanos());
                        observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims").updateOne(new Document("_id.resourceType", "PIPELINE_ACTUATION"),
                                new Document("$set", new Document("leaseUntil", new java.util.Date(0))));
                    } else if (scenario.equals("owner")) {
                        var current = raw.read(pass.claim().key()).orElseThrow().claim();
                        assertThat(raw.release(current)).isTrue();
                        assertThat(raw.acquire(current.key(), new WorkloadOwner("replacement", "replacement-boot"), current.topologyRevision(), Duration.ofSeconds(30)).acquired()).isTrue();
                        offset.addAndGet(Duration.ofSeconds(10).toNanos()); ownership.renewDue();
                    } else if (scenario.equals("topology")) {
                        var current = raw.read(pass.claim().key()).orElseThrow().claim();
                        assertThat(raw.acquire(current.key(), current.owner(), 2, Duration.ofSeconds(30)).acquired()).isTrue();
                        membership.install(new ClusterMembership(business.clusterId(), 2, Set.of(memberOwner.nodeId())));
                        offset.addAndGet(Duration.ofSeconds(10).toNanos()); ownership.renewDue();
                    } else if (scenario.equals("execution")) {
                        assertThat(ownership.beginExecution(scope, pass.claim()).allowed()).isTrue();
                    }
                    if (!scenario.equals("blocked")) return;
                    var admin = observer.getDatabase("admin");
                    long count = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                            .append("data", new Document("appName", MongoConnection.processTag(process)).append("failCommands", List.of("find"))
                                    .append("blockConnection", true).append("blockTimeMS", 12000))).get("count")).longValue();
                    blocked.set(holder.submit(() -> main.getDatabase(storage.cacheDatabase()).getCollection("workload_claims")
                            .withTimeout(18, TimeUnit.SECONDS).find(new Document("_id.resourceType", "PIPELINE_ACTUATION")).first()));
                    admin.runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", count + 1).append("maxTimeMS", 3000));
                }, ignored -> {}, (p, m) -> {}, (original, command) -> new ExecutionPlanRecorder() {
                    public void record(ExecutionPlan value) {} public void forget(String id) {} public void forget(ExecutionPlan value) {}
                }, original -> NativeBusinessContext.localCapturePreparationLease(false, properties.getProfile(), HazelcastProperties.DiscoveryMode.NONE,
                        StorageBinding.Mode.OP, instance, membership, ownership, original));
                var actuator = new ScopedEngineLifecycleActuator(pass, engine, dags, captures, ownership, storage.cacheDatabase(), ConnectorReadiness.NONE,
                        Clock.systemUTC(), ports, new ScopedEngineLifecycleActuator.Submissions());
                long start = System.nanoTime(); Throwable failed = catchThrowable(() -> actuator.start(pipeline.id()));
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                var currentKey = new WorkloadClaimKey(business.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline.id());
                var reading = new MongoWorkloadClaimStore(observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims")).read(currentKey).orElseThrow();
                boolean localLive = ownership.hasLocalWriterLease(new ExecutionFence(scope, pass.claim()));
                System.out.printf("CAPTURE_GUARD_POOL scenario=%s elapsed_ms=%d failure=%s capture_reached=%s same_original=%s durable_live=%s durable_remaining_ms=%d local_conservative_live=%s%n",
                        scenario, elapsed, failed == null ? "none" : failed.getClass().getSimpleName(), opened.get(), WorkloadClaimFence.from(reading.claim()).equals(pass.claim()),
                        reading.leased(), reading.leaseRemaining().toMillis(), localLive);
                if (failed instanceof TapstateException) {
                    for (var frame : failed.getStackTrace()) if (frame.getClassName().endsWith("ScopedEngineLifecycleActuator"))
                        System.out.println("CAPTURE_GUARD_FAILURE_FRAME " + frame.getMethodName() + ":" + frame.getLineNumber());
                }
                if (blocked.get() != null) blocked.get().get(18, TimeUnit.SECONDS);
                for (var record : records) if (((Number) record.get("execution_ms")).doubleValue() >= 4000) {
                    var mongoStats = (Map<?, ?>) record.get("mongo"); var pool = (Map<?, ?>) record.get("pool");
                    System.out.printf("CAPTURE_GUARD_IO execution_ms=%s queue_ms=%s wire=%s checkout_failures=%s checkout_ms=%s%n",
                            record.get("execution_ms"), record.get("queue_ms"), mongoStats.get("started"), pool.get("failures"), pool.get("checkout_ms"));
                }
                store.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(3));
                logger.detachAppender(appender); appender.stop(); logger.setLevel(originalLevel);
                observer.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                if (scenario.equals("blocked")) {
                    assertThat(reading.leased()).isTrue(); assertThat(localLive).isTrue(); assertThat(WorkloadClaimFence.from(reading.claim())).isEqualTo(pass.claim());
                    assertThat(opened).isTrue(); assertThat(failed).isInstanceOf(CaptureReached.class);
                } else {
                    assertThat(opened.get()).isEqualTo(scenario.equals("after-capture-owner"));
                    assertThat(failed).isInstanceOfSatisfying(TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                    var before = observer.getDatabase(storage.cacheDatabase()).getCollection("pipeline_state").find(new Document("_id", pipeline.id())).first();
                    assertThatThrownBy(() -> cache.pipelineStates(scope, pass.claim(), () -> {}).compareAndSwap(pipeline.id(), 0, "RUNNING", Instant.now()))
                            .isInstanceOfSatisfying(TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                    assertThat(observer.getDatabase(storage.cacheDatabase()).getCollection("pipeline_state").find(new Document("_id", pipeline.id())).first()).isEqualTo(before);
                    verify(engine, never()).submitRun(anyString(), any(), anyMap(), any(), any(OperatorRunManifest.class));
                }
            }
        }
    }
    static final class CaptureReached extends RuntimeException {}
}
