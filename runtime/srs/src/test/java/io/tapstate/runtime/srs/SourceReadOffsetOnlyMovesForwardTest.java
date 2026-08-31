package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The persisted source read offset never moves backwards.
 *
 * <p>The offset is written down as the source's own token, but it is <em>ranked</em> by the order the
 * engine assigned as it read — the pair (generation, ring sequence) — and it is clamped so it never
 * passes the slowest consumer's durably acked position. That clamp is where a rewind comes from: the
 * minimum it resolves to can fall, while nothing about the falling is visible at the time. A second
 * pipeline joining a shared chain with a sink further behind is enough, and so is a restart, which
 * raises the generation while every consumer's acked position still sits in the generation before it.
 *
 * <p>Nothing about a rewind announces itself: the write succeeds, the run keeps going, and the loss
 * shows up only on the next restart, which resumes from the earlier position and re-mines — or, once the
 * source has aged past it, cannot.
 *
 * <p>What stops it is the store's own guarantee that this value only ever moves forward. The guarantee
 * is on the store rather than on its callers because the caller resolving the rewinding candidate is
 * behaving correctly: clamping to the slowest sink is exactly what keeps unacked changes re-minable. The
 * fake here honours that contract, as any implementation must; the real one is held to it against a live
 * database by {@code MongoSrsMetaStoreIT}.
 */
class SourceReadOffsetOnlyMovesForwardTest {

    private static HazelcastInstance hz;

    /** The generation the first run's ring is running under; the restart runs under the next one. */
    private static final long FIRST_GENERATION = 1L;
    private static final long RESTARTED_GENERATION = 2L;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-offset-forward-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    @Test
    void doesNotRewindWhenASlowerConsumerJoinsAndDropsTheClamp() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.forward-join")));
        AdvanceOnlyMeta meta = new AdvanceOnlyMeta();

        // One consumer, acked through the fifth change, so the offset reaches s5.
        runOnce(gate, meta, FIRST_GENERATION, 5, List.of(ackedAt("p1", FIRST_GENERATION, 4, "s5")));
        assertThat(meta.current()).isEqualTo("s5");

        // A second pipeline joins the shared chain and its sink is further behind. The clamp now resolves
        // to its position, which is a place this chain has already read past.
        runOnce(gate, meta, FIRST_GENERATION, 2, List.of(
                ackedAt("p1", FIRST_GENERATION, 4, "s5"),
                ackedAt("p2", FIRST_GENERATION, 1, "s2")));

        assertThat(meta.current())
                .as("persisted offset after the slower consumer joined; every advance in order was %s",
                        meta.advances)
                .isEqualTo("s5");
    }

    @Test
    void doesNotRewindWhenARestartRaisesTheGenerationAboveEveryAckedPosition() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.forward-restart")));
        AdvanceOnlyMeta meta = new AdvanceOnlyMeta();

        runOnce(gate, meta, FIRST_GENERATION, 5, List.of(ackedAt("p1", FIRST_GENERATION, 4, "s5")));
        assertThat(meta.current()).isEqualTo("s5");

        // The restart: a new generation, so every change it reads outranks every acked position from the
        // generation before -- and the clamp therefore resolves to one of those older acks, whose token is
        // a place this chain read past before the restart.
        runOnce(gate, meta, RESTARTED_GENERATION, 3, List.of(ackedAt("p1", FIRST_GENERATION, 2, "s3")));

        assertThat(meta.current())
                .as("persisted offset after the restart; every advance in order was %s", meta.advances)
                .isEqualTo("s5");
    }

    /**
     * Drives one cdc run of {@code changes} inserts, as a restarted process would. The source states each
     * change's position itself -- s1, s2, ... -- which is what a real one does and what the run under test
     * now reads; nothing on the write side hands positions out any more.
     */
    private static void runOnce(
            SrsWriteGate gate, SrsMetaStore meta, long generation, int changes, List<ConsumerOffset> consumers) {
        CdcChain chain = new CdcChain(gate, meta, "chain", generation, 0L);
        List<Envelope> events = new ArrayList<>();
        for (int i = 1; i <= changes; i++) {
            events.add(Envelope.insert(i, "orders", Map.of("id", i), Map.of()));
        }
        CdcPhase.run(new FakeCdcPort(events), new CaptureConfig("mysql", Map.of(), List.of("orders")), chain,
                () -> Long.MAX_VALUE, () -> consumers, new CaptureHealth());
    }

    /** One consumer whose sink has durably landed everything up to the given position. */
    private static ConsumerOffset ackedAt(String pipelineId, long generation, long seq, String token) {
        return new ConsumerOffset(
                pipelineId, Map.of("orders", seq), new ChainPosition(new SourceOrder(generation, seq), token));
    }

    /**
     * A meta store holding only the source read offset, and honouring the one guarantee its contract makes
     * about it: it only ever moves forward. A position that does not rank after the recorded one is
     * ignored, silently and successfully.
     */
    private static final class AdvanceOnlyMeta implements SrsMetaStore {
        ChainPosition recorded;
        final List<String> advances = new ArrayList<>();

        String current() {
            return recorded == null ? null : recorded.token();
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            advances.add(position.token());
            if (recorded != null && position.order().compareTo(recorded.order()) <= 0) {
                return;
            }
            recorded = position;
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException();
        }
    }

    /** A cdc port replaying a fixed list of change events, stating a position for each the way a source does. */
    private static final class FakeCdcPort implements CapturePort {
        private final List<Envelope> events;

        FakeCdcPort(List<Envelope> events) {
            this.events = events;
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            int n = 0;
            for (Envelope e : events) {
                listener.onEvent(e, Optional.of(new SourcePosition("s" + ++n)));
            }
            return () -> {
            };
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            throw new UnsupportedOperationException();
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
