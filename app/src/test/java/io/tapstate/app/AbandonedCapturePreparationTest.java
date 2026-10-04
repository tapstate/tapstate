package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SnapshotWorkers;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises real capture preflight and snapshot reservation without activating a connector read. */
class AbandonedCapturePreparationTest {
    private static final String PIPELINE = "pipeline", SOURCE = "source", TABLE = "orders";
    private static HazelcastInstance member;
    private static final InMemorySrsLogStore LOG = new InMemorySrsLogStore();

    @BeforeAll
    static void startMember() throws Exception {
        Config config = new Config();
        config.setClusterName("abandoned-capture-preparation-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        try (ServerSocket socket = new ServerSocket(0)) {
            config.getNetworkConfig().setPort(socket.getLocalPort()).setPortAutoIncrement(false);
        }
        config.addRingBufferConfig(new RingbufferConfig("srs.*").setCapacity(8)
                .setBackupCount(0).setInMemoryFormat(InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setEnabled(true)
                        .setFactoryImplementation(new SrsLogRingbufferStoreFactory(LOG))));
        member = Hazelcast.newHazelcastInstance(config);
        member.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, LOG);
    }

    @AfterAll
    static void stopMember() { if (member != null) { member.shutdown(); } }

    @Test
    void anAbandonedReservationCanPrepareAgainWithoutDiscardingItsProgress() {
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            Fixture fixture = new Fixture("abandoned-reservation", workers);
            long previousEpoch = fixture.seedTrustedPreviousRun(List.of(TABLE));
            List<String> published = fixture.meta.captureServingTables(fixture.chain.value());
            ChainPosition sourceRead = fixture.meta.read(fixture.chain.value()).orElseThrow().sourceRead();
            fixture.meta.upsertConsumerOffset(fixture.chain.value(), new ConsumerOffset(SrsConsumerId.of(PIPELINE, SOURCE).value(),
                    Map.of(TABLE, -1L), sourceRead, List.of(TABLE), "retained-snapshot-seam", previousEpoch,
                    Map.of(TABLE, sourceRead), io.tapstate.spi.store.ConsumerProgressKind.SRS));
            ConsumerOffset previous = fixture.meta.consumerOffsets(fixture.chain.value()).getFirst();
            StoreBackedPipelineCaptureCoordinator captures = fixture.captures(CaptureOwnership.single());
            try {
                captures.startCapture(PIPELINE, fixture.store.artifacts(), "aborted-cursor");
                assertThat(captures.isActive(PIPELINE)).isTrue();
                assertThat(fixture.starts).hasValue(1);
                assertThat(fixture.meta.read(fixture.chain.value()).orElseThrow().epoch()).isEqualTo(previousEpoch + 1);
                assertThat(fixture.buffer.hasSnapshot(PIPELINE, fixture.ringName(), "aborted-cursor")).isTrue();
                assertThat(fixture.meta.captureServingTables(fixture.chain.value())).isEqualTo(published);

                // Admission failed after preparation, before submission could activate the reserved snapshot.
                captures.stopCapture(PIPELINE, false);
                assertThat(captures.isActive(PIPELINE)).isFalse();
                assertThat(fixture.chains.isProvisioned(fixture.chain)).isFalse();
                assertThat(fixture.buffer.hasSnapshot(PIPELINE, fixture.ringName())).isFalse();
                assertThat(fixture.meta.captureServingTables(fixture.chain.value())).isEqualTo(published);
                assertThat(fixture.meta.read(fixture.chain.value()).orElseThrow().sourceRead()).isEqualTo(sourceRead);
                ConsumerOffset retained = fixture.meta.consumerOffsets(fixture.chain.value()).getFirst();
                assertThat(retained.sinkAcked()).isEqualTo(previous.sinkAcked());
                assertThat(retained.snapshotCompletedTables()).isEqualTo(previous.snapshotCompletedTables());
                try (var one = workers.reserve().orElseThrow(); var two = workers.reserve().orElseThrow()) {
                    assertThat(workers.reserve()).as("abandoned preparation returned both bounded capacity slots").isEmpty();
                }

                assertThatCode(() -> captures.startCapture(PIPELINE, fixture.store.artifacts(), "recovery-cursor"))
                        .as("a stopped reservation can prepare its replacement despite an older published selection")
                        .doesNotThrowAnyException();
                assertThat(captures.isActive(PIPELINE)).isTrue();
                assertThat(fixture.starts).hasValue(2);
                assertThat(fixture.meta.read(fixture.chain.value()).orElseThrow().epoch()).isEqualTo(previousEpoch + 2);
                assertThat(fixture.buffer.hasSnapshot(PIPELINE, fixture.ringName(), "recovery-cursor")).isTrue();
                assertThat(fixture.meta.captureServingTables(fixture.chain.value()))
                        .as("preparation has not falsely published a physical tail").isEqualTo(published);
                assertThat(fixture.meta.read(fixture.chain.value()).orElseThrow().sourceRead()).isEqualTo(sourceRead);
                assertThat(fixture.reads).hasValue(0);
            } finally {
                captures.stopCapture(PIPELINE, false);
                captures.close();
            }
        }
    }

    @Test
    void anUnservedRequestedTableStillWaitsForItsOtherOwner() {
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            Fixture fixture = new Fixture("current-selection-other-owner", workers);
            long epoch = fixture.seedTrustedPreviousRun(List.of("customers"));
            List<String> published = fixture.meta.captureServingTables(fixture.chain.value());
            ClusterProperties properties = new ClusterProperties();
            properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            ClusterMembershipGate gate = new ClusterMembershipGate(properties);
            gate.install(new ClusterMembership("cluster", 7, Set.of("local", "remote", "third")));
            assertThat(gate.canCommit(Set.of("local", "remote"))).isTrue();
            InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
            WorkloadClaimKey key = new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE,
                    new StoreBackedPipelineCaptures(fixture.store).captureIds(PIPELINE).getFirst());
            WorkloadClaim remote = claims.acquire(key, new WorkloadOwner("remote", "remote-boot"),
                    7, Duration.ofSeconds(30)).claim();
            CaptureOwnership local = new CaptureOwnership("cluster", new WorkloadOwner("local", "local-boot"),
                    gate, new ClusterWorkloadClaims(claims, gate), Duration.ofSeconds(30));
            StoreBackedPipelineCaptureCoordinator captures = fixture.captures(local);
            try {
                assertThatThrownBy(() -> captures.startCapture(PIPELINE))
                        .as("the current owner must publish the missing table before attachment preparation")
                        .isInstanceOf(RingNotOpenYet.class);
                assertThat(captures.isActive(PIPELINE)).isFalse();
                assertThat(fixture.starts).as("production preflight refused before the run unit was entered").hasValue(0);
                assertThat(fixture.reads).hasValue(0);
                assertThat(fixture.meta.read(fixture.chain.value()).orElseThrow().epoch()).isEqualTo(epoch);
                assertThat(fixture.meta.captureServingTables(fixture.chain.value())).isEqualTo(published);
                assertThat(fixture.meta.captureTables(fixture.chain.value()))
                        .as("the request adds this table without dropping the owner's prior selection")
                        .containsExactly("customers", TABLE);
                assertThat(claims.read(key).orElseThrow().claim()).isEqualTo(remote);
            } finally {
                captures.stopCapture(PIPELINE, false);
                captures.close();
            }
        }
    }

    private static final class Fixture {
        final InMemoryStorePort store;
        final SrsMetaStore meta;
        final SrsCoordinator chains;
        final SnapshotBuffer buffer = new SnapshotBuffer();
        final AtomicInteger starts = new AtomicInteger(), reads = new AtomicInteger();
        final MiningChainId chain;
        final CaptureRunUnit unit;

        Fixture(String key, SnapshotWorkers workers) {
            SourceResource source = new SourceResource(SOURCE, null, "mysql", Map.of("host", "in-memory"),
                    SourceMode.CDC, List.of(TableRef.literal(TABLE)), new Srs(key, null, null, null, true), null);
            PipelineResource pipeline = new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE, true)),
                    null, null, new ServeBlock.Inline(null, FromRef.literal(SOURCE),
                            List.of(new SyncElement("sync", SOURCE, null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "latest"), null);
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            artifacts.save(source); artifacts.save(pipeline);
            store = new InMemoryStorePort(artifacts, LOG); meta = store.meta(); chains = new SrsCoordinator(meta);
            chain = MiningChainId.resolve(SourceCaptureResolution.of(source).config(), key);
            CapturePort neverRead = new CapturePort() {
                @Override public CaptureBatch snapshot(CaptureConfig config) {
                    reads.incrementAndGet(); throw new AssertionError("reserved snapshot must not read before activation");
                }
                @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                    reads.incrementAndGet(); throw new AssertionError("reserved capture must not open its tail before activation");
                }
                @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
                @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
            };
            unit = new CaptureRunUnit(neverRead, chains, meta, member, buffer, workers);
        }

        long seedTrustedPreviousRun(List<String> physicalTables) {
            meta.create(chain.value(), null);
            long epoch = meta.openEpoch(chain.value());
            ChainPosition anchor = new ChainPosition(new SourceOrder(epoch, -1L), "trusted-source-position");
            meta.requestCaptureTables(chain.value(), physicalTables);
            assertThat(meta.publishCaptureTables(chain.value(), epoch, physicalTables)).isTrue();
            meta.advanceCaptureCheckpoint(chain.value(), anchor, physicalTables);
            return epoch;
        }

        StoreBackedPipelineCaptureCoordinator captures(CaptureOwnership ownership) {
            CaptureAttacher attacher = (spec, receive, startTail) -> {
                starts.incrementAndGet();
                return unit.start(spec, receive, startTail);
            };
            return new StoreBackedPipelineCaptureCoordinator(store, attacher, chains, buffer, ownership, Duration.ZERO);
        }

        String ringName() { return SrsRingbuffer.ringName(chain.value(), TABLE); }
    }
}
