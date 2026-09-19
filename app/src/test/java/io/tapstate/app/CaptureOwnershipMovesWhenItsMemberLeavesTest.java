package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The member that takes a capture over carries on from where the one that left had reached.
 *
 * <p>Two things have to be true together and only one of them is visible in a final row count. The
 * capture has to change hands at all - a source nobody is reading is a pipeline that has silently
 * stopped - and the member that picks it up has to start where the durable record says, rather than at
 * the source's present moment. Starting at the present is the one that hides: the tail comes up
 * healthy, the claim is held, every reading says the pipeline is running, and every change made
 * between the two members is simply gone. Re-reading from the beginning hides just as well in the
 * other direction, since an idempotent sink absorbs the repeat and the final state is the same.
 *
 * <p>So what is asserted is the <em>start position</em> handed to the member that took over, against
 * the position the durable record held when the first member went away. A count at either end cannot
 * tell those three histories apart; this can.
 *
 * <p><b>Why two real members rather than two runs.</b> Restarting one member already has a case of its
 * own a level down, and it passes on a record that never leaves the process. What this adds is that
 * the position is read by a <em>different</em> member than the one that wrote it, with the writer gone
 * - the cross-member half, which a single member cannot show because a member-local record and a
 * durable one behave identically until somebody else has to read it.
 *
 * <p><b>What stands in, and what does not.</b> The consumer's acknowledgement is stood in for: these
 * runs have no sink, and the frontier only advances as far as the slowest consumer has confirmed, so
 * with nothing confirming, nothing would ever be recorded to resume from - and the case would assert
 * a resume against a position the product never wrote. The acknowledgement is put far ahead, which
 * leaves the recorded position decided by how far the capture actually read. Everything else is the
 * product: the claim that moves ownership, the record the capture keeps, and the start the run that
 * takes over resolves from it.
 */
class CaptureOwnershipMovesWhenItsMemberLeavesTest {

    private static final String SOURCE_ID = "orders_src";
    private static final String PIPELINE = "orders_pipe";
    private static final String TABLE = "orders";
    private static final String CLUSTER = "cluster-a";

    private static final Duration TTL = Duration.ofSeconds(30);
    /** Long enough that nothing renews during this case: the member that leaves must lose its lease. */
    private static final Duration RENEW = Duration.ofHours(1);
    private static final Duration BUDGET = Duration.ofSeconds(20);

    private HazelcastInstance first;
    private HazelcastInstance second;

    @BeforeEach
    void startTwoMembers() throws IOException {
        int[] ports = twoFreePorts();
        String cluster = "capture-handover-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(config(cluster, ports[0], ports));
        second = Hazelcast.newHazelcastInstance(config(cluster, ports[1], ports));
        awaitMembers(first, 2);
    }

    @AfterEach
    void stopBothMembers() {
        for (HazelcastInstance member : List.of(first, second)) {
            if (member != null && member.getLifecycleService().isRunning()) {
                member.shutdown();
            }
        }
    }

    @Test
    void theMemberThatTakesTheCaptureOverResumesWhereTheOneThatLeftHadReached() {
        InMemoryStorePort store = new InMemoryStorePort(artifacts());
        SrsMetaStore meta = store.meta();
        InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        ClusterMembershipGate gate = eligibleGate();

        // The member that reads first, and the handle its run is behind - taken from the attach so the
        // chain it mines can be named, which is how the durable record is found.
        GatedSource leaving = new GatedSource();
        SrsCoordinator srsFirst = new SrsCoordinator(meta);
        CaptureRunUnit unitFirst = new CaptureRunUnit(leaving, srsFirst, meta, first);
        AtomicReference<CaptureRun> runOnTheMemberThatLeaves = new AtomicReference<>();
        AtomicReference<CaptureRunSpec> specItRan = new AtomicReference<>();
        StoreBackedPipelineCaptureCoordinator onFirst = managed(store, srsFirst,
                (spec, passthrough, startTail) -> {
                    specItRan.set(spec);
                    CaptureRun run = unitFirst.start(spec, passthrough, startTail);
                    runOnTheMemberThatLeaves.set(run);
                    return run;
                },
                gate, claims, new WorkloadOwner("node-a", "boot-a"));

        onFirst.startCapture(PIPELINE);
        String chain = runOnTheMemberThatLeaves.get().chainId().orElseThrow().value();

        // A consumer that has confirmed everything it will ever be sent. Without one the frontier has
        // nothing to be bounded by and the capture records no position at all, which would leave the
        // assertion below comparing a resume against nothing.
        leaving.feed(change(1));
        await("the chain to be opened by the capture that started", () -> meta.read(chain).isPresent());
        SrsMeta opened = meta.read(chain).orElseThrow();
        meta.advanceSinkAcked(chain, PIPELINE,
                new ChainPosition(new SourceOrder(opened.epoch(), Long.MAX_VALUE / 2), "src-far"));

        leaving.feed(change(2));
        await("the capture to record how far it has read",
                () -> meta.read(chain).map(SrsMeta::sourceReadOffset).isPresent());
        String reached = meta.read(chain).orElseThrow().sourceReadOffset();
        assertThat(reached)
                .describedAs("the durable record holds the position this member read up to, which is "
                        + "the last change it took rather than where the chain was opened")
                .isEqualTo("src-2");

        // The member goes away: no stop, no release, nothing of its own run on the way out. Its lease is
        // then the only thing holding the capture, and it is no longer renewing it.
        leaving.stopFeeding();
        first.getLifecycleService().terminate();
        claims.elapse(TTL.plusSeconds(1));

        GatedSource takingOver = new GatedSource();
        SrsCoordinator srsSecond = new SrsCoordinator(meta);
        CaptureRunUnit unitSecond = new CaptureRunUnit(takingOver, srsSecond, meta, second);
        StoreBackedPipelineCaptureCoordinator onSecond = managed(store, srsSecond, unitSecond::start,
                gate, claims, new WorkloadOwner("node-b", "boot-b"));

        onSecond.startCapture(PIPELINE);

        WorkloadClaimKey capture = new WorkloadClaimKey(
                CLUSTER, WorkloadClaimType.CAPTURE, CaptureId.of(specItRan.get()).value());
        assertThat(claims.read(capture).orElseThrow().claim().owner())
                .describedAs("the capture changed hands: the member that is still here holds it")
                .isEqualTo(new WorkloadOwner("node-b", "boot-b"));
        assertThat(takingOver.startedAt())
                .describedAs("and it carries on from the position the record held, not from the source's "
                        + "present moment - which would come up healthy and lose every change made "
                        + "between the two members - and not from the chain's beginning either")
                .isEqualTo(CaptureStart.resume(new SourcePosition(reached)));

        onSecond.stopCapture(PIPELINE, false);
    }

    private StoreBackedPipelineCaptureCoordinator managed(
            InMemoryStorePort store,
            SrsCoordinator srs,
            CaptureAttacher attacher,
            ClusterMembershipGate gate,
            InMemoryWorkloadClaimStore claims,
            WorkloadOwner owner) {
        CaptureOwnership ownership = new CaptureOwnership(
                CLUSTER, owner, gate, new ClusterWorkloadClaims(claims, gate), TTL);
        return new StoreBackedPipelineCaptureCoordinator(
                store, attacher, srs, new SnapshotBuffer(), ownership, RENEW);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        properties.setBootstrapMinMembers(2);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership(CLUSTER, 3, Set.of("node-a", "node-b")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static InMemoryArtifactStore artifacts() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE_ID, null, "fake", Map.of("host", "db.internal"),
                SourceMode.CDC, List.of(TableRef.literal(TABLE)), null, null));
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE_ID, true)),
                null, null,
                new ServeBlock.Inline(null, FromRef.literal(SOURCE_ID),
                        List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        return artifacts;
    }

    private static Envelope change(int id) {
        return Envelope.insert(id, TABLE, Map.of("id", (long) id), Map.of());
    }

    private static void await(String what, java.util.function.BooleanSupplier condition) {
        long deadline = System.nanoTime() + BUDGET.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep();
        }
        throw new AssertionError("timed out after " + BUDGET + " waiting for " + what);
    }

    private static void awaitMembers(HazelcastInstance member, int size) {
        await("the members to find each other", () -> member.getCluster().getMembers().size() == size);
    }

    private static void sleep() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", interrupted);
        }
    }

    private static Config config(String cluster, int port, int[] ports) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .setMembers(List.of("127.0.0.1:" + ports[0], "127.0.0.1:" + ports[1]));
        config.getJetConfig().setEnabled(false);
        // One synchronous backup, which is what a clustered member configures: the ring must outlive the
        // member that owned its partition, or terminating one would take the chain with it.
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(64)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(1));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        return config;
    }

    private static int[] twoFreePorts() throws IOException {
        try (ServerSocket one = new ServerSocket(0); ServerSocket other = new ServerSocket(0)) {
            return new int[] {one.getLocalPort(), other.getLocalPort()};
        }
    }

    /** A source fed by the case, recording the start it was asked to read from. */
    private static final class GatedSource implements CapturePort {

        private final LinkedBlockingQueue<Envelope> pending = new LinkedBlockingQueue<>();
        private final AtomicReference<CaptureStart> started = new AtomicReference<>();
        private volatile boolean running;
        private Thread daemon;

        void feed(Envelope change) {
            pending.add(change);
        }

        void stopFeeding() {
            pending.clear();
        }

        CaptureStart startedAt() {
            return started.get();
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            throw new UnsupportedOperationException("this case drives a cdc-only pipeline");
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            started.set(start);
            running = true;
            daemon = new Thread(() -> {
                while (running) {
                    try {
                        Envelope change = pending.poll(25, TimeUnit.MILLISECONDS);
                        if (change != null) {
                            listener.onBatch(List.of(change),
                                    Optional.of(new SourcePosition("src-" + change.ts())));
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (RuntimeException gone) {
                        // The member this run belongs to was terminated under it. A dead member's tail
                        // failing is the scenario, not a fault to report.
                        return;
                    }
                }
            }, "handover-source-cdc");
            daemon.setDaemon(true);
            daemon.start();
            return () -> {
                running = false;
                daemon.interrupt();
            };
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }
}
