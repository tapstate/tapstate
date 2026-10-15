package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoClusterProfileStore;
import io.tapstate.adapters.mongostore.MongoSrsLogStore;
import io.tapstate.adapters.mongostore.MongoSrsMetaStore;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.*;
import io.tapstate.runtime.srs.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;

/** Real Mongo startup accounting across a physical reader takeover; the source callback is controlled. */
@RequiresDocker
class AProfiledCaptureTakeoverKeepsItsStartupProofIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final String CLUSTER = "takeover-cluster";
    private static final String PIPELINE = "pipeline";
    private static final String SOURCE = "source";
    private static final String TABLE = "orders";
    private static final Duration TTL = Duration.ofMinutes(5);

    @Test
    void anAcceptedReplacementReaderCompletesTheOriginalPreparedStartupAtItsNewEpoch() {
        try (Fixture fixture = new Fixture(false)) {
            fixture.takeOver();
            fixture.port.deliver();
            await().atMost(Duration.ofSeconds(10)).until(() -> fixture.meta.captureReadState(fixture.chain)
                    .map(CaptureReadState::accepted).orElse(false));
            fixture.assertSamePipelineExecution();
            assertThat(fixture.coordinator.startupProofs(PIPELINE, fixture.pipelineFence())).containsKey(SOURCE);
            var proof = fixture.coordinator.startupProofs(PIPELINE, fixture.pipelineFence()).get(SOURCE);
            assertThat(proof.readerState().attempt().chainEpoch()).isGreaterThan(fixture.initialEpoch);
            assertThat(proof.requestedPosition().position().token()).isEqualTo("initial");
        }
    }

    @Test
    void aReplacementRejectedBeforeAcceptanceKeepsItsQualifiedFailureOnTheCurrentStartup() {
        try (Fixture fixture = new Fixture(true)) {
            fixture.takeOver();
            await().atMost(Duration.ofSeconds(10)).until(() -> fixture.meta.captureReadState(fixture.chain)
                    .map(CaptureReadState::failed).orElse(false));
            fixture.assertSamePipelineExecution();
            assertThat(fixture.coordinator.startupFailures(PIPELINE, fixture.pipelineFence())).containsKey(SOURCE);
            var failure = fixture.coordinator.startupFailures(PIPELINE, fixture.pipelineFence()).get(SOURCE);
            assertThat(failure.code()).isEqualTo(CaptureError.START_FROM_OUTSIDE_WINDOW.code());
            assertThat(failure.requestedPosition().position().token()).isEqualTo("initial");
        }
    }

    @Test
    void aRetiredFirstConsumerCannotDisplaceTheStillCurrentConsumersTakeoverProof() {
        try (Fixture fixture = new Fixture(false)) {
            WorkloadClaim other = fixture.attachAnother("other");
            fixture.coordinator.runOnlyWhileDriving(id -> !id.equals(PIPELINE));
            assertThat(fixture.workloads.release(fixture.pipeline)).isTrue();
            fixture.takeOver();
            fixture.port.deliver();
            await().atMost(Duration.ofSeconds(10)).until(() -> fixture.meta.captureReadState(fixture.chain)
                    .map(CaptureReadState::accepted).orElse(false));
            assertThat(fixture.coordinator.startupProofs("other", WorkloadClaimFence.from(other))).containsKey(SOURCE);
            assertThat(fixture.coordinator.startupProofs(PIPELINE, fixture.pipelineFence())).isEmpty();
            assertThat(fixture.workloads.read(other.key()).orElseThrow().claim().executionGeneration())
                    .isEqualTo(other.executionGeneration());
        }
    }

    @Test
    void anAcquisitionTopologyRefreshDoesNotInvalidateTheSameExecutionsTakeover() {
        try (Fixture fixture = new Fixture(false)) {
            fixture.currentPipeline = fixture.workloads.acquire(fixture.pipeline.key(), fixture.pipeline.owner(), 2, TTL).claim();
            assertThat(fixture.currentPipeline.topologyRevision()).isEqualTo(2);
            fixture.assertSamePipelineExecution();
            fixture.takeOver();
            fixture.port.deliver();
            await().atMost(Duration.ofSeconds(10)).until(() -> fixture.meta.captureReadState(fixture.chain)
                    .map(CaptureReadState::accepted).orElse(false));
            assertThat(fixture.coordinator.startupProofs(PIPELINE, fixture.pipelineFence())).containsKey(SOURCE);
        }
    }

    @Test
    void aNewPipelineOpeningTheReplacementReaderAlsoQualifiesTheExistingJoinersFailure() {
        try (Fixture fixture = new Fixture(true)) {
            WorkloadClaim producer = fixture.prepareAnother("producer");
            assertThat(fixture.workloads.release(fixture.remoteCapture)).isTrue();
            assertThatThrownBy(() -> fixture.coordinator.startCapture("producer", fixture.store.artifacts(), Map.of(),
                    WorkloadClaimFence.from(producer))).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(CaptureError.START_FROM_OUTSIDE_WINDOW));
            assertThat(fixture.coordinator.startupFailures("producer", WorkloadClaimFence.from(producer))).containsKey(SOURCE);
            fixture.assertSamePipelineExecution();
            assertThat(fixture.coordinator.startupFailures(PIPELINE, fixture.pipelineFence())).containsKey(SOURCE);
        }
    }

    @Test
    void aDirectTakeoverUsesItsConfirmedPrefixAndKeepsNonSharedStartupQualification() {
        try (Fixture fixture = new Fixture(false, false)) {
            fixture.takeOver();
            fixture.port.deliver();
            await().atMost(Duration.ofSeconds(10)).until(() -> fixture.meta.captureReadState(fixture.chain)
                    .map(CaptureReadState::accepted).orElse(false));
            fixture.assertSamePipelineExecution();
            assertThat(fixture.coordinator.startupProofs(PIPELINE, fixture.pipelineFence())).containsKey(SOURCE);
            assertThat(fixture.meta.read(fixture.chain).orElseThrow().consumerOffset(
                    SrsConsumerId.of(PIPELINE, SOURCE).value()).orElseThrow().progressKind())
                    .isEqualTo(ConsumerProgressKind.DIRECT_SOURCE);
        }
    }

    @Test
    void anUncodedDirectReaderFailureReachesOnlyItsCurrentConsumerExecution() {
        try (Fixture fixture = new Fixture(false, false)) {
            fixture.takeOver();
            fixture.port.deliver();
            IllegalStateException failure = new IllegalStateException("the replacement direct reader failed");
            fixture.port.listener.onError(failure);
            WorkloadClaimFence current = fixture.pipelineFence();
            assertThat(fixture.coordinator.captureFailure(PIPELINE, current)).containsSame(failure);
            assertThat(fixture.coordinator.captureFailure(PIPELINE)).containsSame(failure);
            assertThat(fixture.coordinator.captureFailure(PIPELINE, new WorkloadClaimFence(current.key(),
                    current.owner(), current.claimGeneration(), current.executionGeneration() + 1,
                    current.topologyRevision(), current.profileGeneration()))).isEmpty();
            assertThat(fixture.coordinator.captureFailure(PIPELINE, new WorkloadClaimFence(current.key(),
                    current.owner(), current.claimGeneration() + 1, current.executionGeneration(),
                    current.topologyRevision(), current.profileGeneration()))).isEmpty();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
        final String database = "takeover_" + UUID.randomUUID().toString().substring(0, 8);
        final com.mongodb.client.MongoDatabase db = client.getDatabase(database);
        final MongoClusterProfileStore profiles = new MongoClusterProfileStore(client,
                db.getCollection("profiles"), db.getCollection("claims"), db.getCollection("nodes"));
        final MongoWorkloadClaimStore workloads = new MongoWorkloadClaimStore(db.getCollection("claims"), profiles);
        final MongoSrsMetaStore meta = new MongoSrsMetaStore(client, db.getCollection("meta"),
                db.getCollection("consumers"), db.getCollection("claims"));
        final MongoSrsLogStore log = new MongoSrsLogStore(client, db.getCollection("log"), db.getCollection("claims"));
        final ControlledSource port;
        final StorePort store;
        HazelcastInstance member;
        StoreBackedPipelineCaptureCoordinator coordinator;
        final WorkloadClaim pipeline;
        WorkloadClaim currentPipeline;
        final WorkloadClaim remoteCapture;
        final String chain;
        final long initialEpoch;
        final boolean shared;

        Fixture(boolean reject) { this(reject, true); }

        Fixture(boolean reject, boolean shared) {
            this.shared = shared;
            try {
                WorkloadOwner local = new WorkloadOwner("node-a", "boot-a");
                WorkloadOwner remote = new WorkloadOwner("node-c", "boot-c");
                ExecutionProfile profile = new ExecutionProfile(1, Map.of("buildVersion", "test"));
                assertThat(profiles.reserve(CLUSTER, local, URI.create("http://127.0.0.1:1"), profile, TTL).acquired()).isTrue();
                assertThat(profiles.reserve(CLUSTER, remote, URI.create("http://127.0.0.1:2"), profile, TTL).acquired()).isTrue();
                WorkloadClaim acquired = workloads.acquire(new WorkloadClaimKey(CLUSTER,
                        WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE), local, 1, TTL).claim();
                pipeline = workloads.advanceExecution(acquired, 1, Set.of(local.nodeId())).orElseThrow();
                currentPipeline = pipeline;
                InMemoryStorePort backing = new InMemoryStorePort(artifacts(shared), log);
                store = mock(StorePort.class, delegatesTo(backing));
                doReturn(meta).when(store).meta();
                doReturn(log).when(store).srsLog();
                doReturn(workloads).when(store).workloadClaims();
                member = Hazelcast.newHazelcastInstance(config(log));
                member.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, log);
                SrsCoordinator srs = new SrsCoordinator(meta);
                port = new ControlledSource(reject);
                CaptureRunUnit unit = new CaptureRunUnit(port, srs, meta, member);
                ClusterProperties properties = new ClusterProperties();
                properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
                properties.setBootstrapMinMembers(2);
                ClusterMembershipGate gate = new ClusterMembershipGate(properties);
                gate.install(new ClusterMembership(CLUSTER, 1, Set.of(local.nodeId(), remote.nodeId())));
                gate.canCommit(Set.of(local.nodeId(), remote.nodeId()));
                CaptureOwnership ownership = new CaptureOwnership(CLUSTER, local, gate,
                        new ClusterWorkloadClaims(workloads, gate), TTL);
                coordinator = new StoreBackedPipelineCaptureCoordinator(store, unit::begin, srs,
                        new SnapshotBuffer(), ownership, Duration.ofHours(1));
                var witness = coordinator.resumeWitnesses(PIPELINE, store.artifacts()).get(SOURCE);
                chain = witness.miningChainId();
                String captureId = new StoreBackedPipelineCaptures(store).captureIds(PIPELINE).getFirst();
                remoteCapture = workloads.acquire(new WorkloadClaimKey(CLUSTER, WorkloadClaimType.CAPTURE, captureId),
                        remote, 1, TTL).claim();
                meta.create(chain, null);
                initialEpoch = meta.openEpoch(chain);
                assertThat(meta.publishCaptureTables(chain, initialEpoch, List.of(TABLE))).isTrue();
                if (shared) {
                    meta.advanceCaptureCheckpoint(chain, new ChainPosition(new SourceOrder(initialEpoch, 10), "initial"), List.of(TABLE));
                } else {
                    ChainPosition confirmed = new ChainPosition(new SourceOrder(initialEpoch, 10), "initial");
                    meta.advanceSourceReadOffset(chain, confirmed);
                    meta.upsertConsumerOffset(chain, new ConsumerOffset(witness.consumerId(), Map.of(), confirmed,
                            List.of(), null, 0, Map.of(), ConsumerProgressKind.DIRECT_SOURCE));
                }
                CaptureReadAttempt remoteReader = meta.beginCaptureReadAttempt(chain, initialEpoch, List.of(TABLE),
                        CaptureReadAttempt.Kind.RESUME, "initial", null, WorkloadClaimFence.from(remoteCapture)).orElseThrow();
                assertThat(meta.recordCaptureAnchor(remoteReader, "initial")).isTrue();
                assertThat(meta.recordCaptureFirstDelivery(remoteReader)).isTrue();
                coordinator.startCapture(PIPELINE, store.artifacts(), Map.of(), pipelineFence());
                assertThat(port.start).isNull();
                assertThat(coordinator.startupProofs(PIPELINE, pipelineFence())).containsKey(SOURCE);
                if (shared) meta.advanceCaptureCheckpoint(chain,
                        new ChainPosition(new SourceOrder(initialEpoch, 20), "latest"), List.of(TABLE));
                else meta.advanceSourceReadOffset(chain, new ChainPosition(new SourceOrder(initialEpoch, 20), "latest"));
            } catch (RuntimeException | Error failure) {
                try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        WorkloadClaimFence pipelineFence() { return WorkloadClaimFence.from(currentPipeline); }

        WorkloadClaim attachAnother(String id) {
            WorkloadClaim other = prepareAnother(id);
            coordinator.startCapture(id, store.artifacts(), Map.of(), WorkloadClaimFence.from(other));
            assertThat(coordinator.startupProofs(id, WorkloadClaimFence.from(other))).containsKey(SOURCE);
            return other;
        }

        WorkloadClaim prepareAnother(String id) {
            store.artifacts().save(pipeline(id, shared));
            WorkloadClaim acquired = workloads.acquire(new WorkloadClaimKey(CLUSTER,
                    WorkloadClaimType.PIPELINE_ACTUATION, id), pipeline.owner(), 1, TTL).claim();
            WorkloadClaim other = workloads.advanceExecution(acquired, 1, Set.of(pipeline.owner().nodeId())).orElseThrow();
            return other;
        }

        void takeOver() {
            assertThat(workloads.release(remoteCapture)).isTrue();
            coordinator.tailWhatNobodyTails();
            assertThat(port.start).isEqualTo(CaptureStart.resume(new SourcePosition(shared ? "latest" : "initial")));
            assertThat(meta.read(chain).orElseThrow().epoch()).isGreaterThan(initialEpoch);
        }

        void assertSamePipelineExecution() {
            WorkloadClaim held = workloads.read(pipeline.key()).orElseThrow().claim();
            assertThat(held.owner()).isEqualTo(pipeline.owner());
            assertThat(held.claimGeneration()).isEqualTo(pipeline.claimGeneration());
            assertThat(held.executionGeneration()).isEqualTo(pipeline.executionGeneration());
        }

        @Override public void close() {
            try { if (coordinator != null) coordinator.close(); }
            finally {
                try { if (member != null) member.shutdown(); }
                finally {
                    try { db.drop(); }
                    finally { client.close(); }
                }
            }
        }
    }

    private static Config config(SrsLogStore log) {
        Config config = new Config().setClusterName("profiled-capture-proof-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new RingbufferConfig("srs.*").setCapacity(64).setBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.OBJECT).setRingbufferStoreConfig(new RingbufferStoreConfig()
                        .setEnabled(true).setFactoryImplementation(new SrsLogRingbufferStoreFactory(log))));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        return config;
    }

    private static InMemoryArtifactStore artifacts(boolean shared) {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE, null, "controlled", Map.of("host", "fixture"),
                SourceMode.CDC, List.of(TableRef.literal(TABLE)), null, null));
        artifacts.save(pipeline(PIPELINE, shared));
        return artifacts;
    }

    private static PipelineResource pipeline(String id, boolean shared) {
        return new PipelineResource(id, null, List.of(SourceRef.spec(SOURCE, shared)), null, null,
                new ServeBlock.Inline(null, FromRef.literal(SOURCE),
                        List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
    }

    private static final class ControlledSource implements CapturePort {
        final boolean reject;
        CaptureStart start;
        CaptureListener listener;
        ControlledSource(boolean reject) { this.reject = reject; }
        @Override public CaptureBatch snapshot(CaptureConfig config) { throw new UnsupportedOperationException(); }
        @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            this.start = start;
            this.listener = listener;
            if (reject) throw new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW,
                    Map.of("requested", "latest", "earliest", "retained", "retention", "1m"), null);
            if (listener instanceof CaptureStartedListener started) started.onStart(
                    start instanceof CaptureStart.Resume resume ? resume.position() : new SourcePosition("latest"));
            return () -> { };
        }
        void deliver() {
            listener.onBatch(List.of(Envelope.insert(1, TABLE, Map.of("id", 1L), Map.of())),
                    Optional.of(new SourcePosition("after")));
        }
        @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
        @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
    }
}
