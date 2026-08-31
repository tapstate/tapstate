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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The persisted source read offset never moves backwards, across a restart included.
 *
 * <p>A restart is where this can happen at all. The offset is ranked by the order the engine assigned as
 * it read -- the pair (generation, ring sequence) -- while the value written down is the connector token.
 * A restart raises the generation and, with the position supplied by a per-run monotonic watermark rather
 * than by the connector, hands the new run tokens that start over from the beginning. So the order rises
 * while the token falls, the sink-acked clamp sees a rising order and admits the advance, and the durable
 * offset is silently set to a position earlier than one already reached.
 *
 * <p>Nothing about that is visible at the time: the write succeeds, the run keeps going, and the loss
 * shows up only on the next restart, which resumes from the earlier position and re-mines -- or, once the
 * source has aged past it, cannot.
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
    void doesNotRewindThePersistedOffsetWhenARestartStartsItsPositionsOver() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.forward")));
        LastWriteWinsMeta meta = new LastWriteWinsMeta();

        // Run 1 reads five changes and a consumer durably acks all of them, so the offset reaches w5.
        runOnce(gate, meta, FIRST_GENERATION, 5, ackedAt(FIRST_GENERATION, 4, "w5"));
        assertThat(meta.current).isEqualTo("w5");

        // The restart: a new generation, and a watermark that starts its tokens over at w1. It reads
        // fewer changes than the first run did, so its tokens stay strictly below the position already
        // reached -- the two runs' tokens must not be allowed to coincide, or a rewind and a legitimate
        // re-advance to the same place would be the same observation.
        runOnce(gate, meta, RESTARTED_GENERATION, 3, ackedAt(RESTARTED_GENERATION, 7, "w3"));

        // w3 here is a position already passed. Persisting it means the next restart resumes from there
        // and re-mines everything after it -- or, once the source has aged past it, cannot.
        assertThat(meta.current)
                .as("persisted offset after the restart; every advance in order was %s", meta.advances)
                .isEqualTo("w5");
    }

    /** Drives one cdc run of {@code changes} inserts over its own fresh watermark, as a restarted process would. */
    private static void runOnce(
            SrsWriteGate gate, SrsMetaStore meta, long generation, int changes, List<ConsumerOffset> consumers) {
        CdcChain chain = new CdcChain(gate, meta, "chain", monotonicWatermark(), generation, 0L);
        List<Envelope> events = new ArrayList<>();
        for (int i = 1; i <= changes; i++) {
            events.add(Envelope.insert(i, "orders", Map.of("id", i), Map.of()));
        }
        FakeCdcPort port = new FakeCdcPort(events);
        CdcPhase.run(port, new CaptureConfig("mysql", Map.of(), List.of("orders")), chain,
                () -> Long.MAX_VALUE, () -> consumers, new CaptureHealth());
    }

    /** One consumer whose sink has durably landed everything up to the given position. */
    private static List<ConsumerOffset> ackedAt(long generation, long seq, String token) {
        return List.of(new ConsumerOffset(
                "p1", Map.of("orders", seq), new ChainPosition(new SourceOrder(generation, seq), token)));
    }

    /** A mock cdc watermark: w1, w2, ... per run -- a restarted process starts a fresh one at w1. */
    private static Supplier<SourcePosition> monotonicWatermark() {
        AtomicLong n = new AtomicLong();
        return () -> new SourcePosition("w" + n.incrementAndGet());
    }

    /** A meta store keeping only the current offset, the way a {@code $set} on one field does. */
    private static final class LastWriteWinsMeta implements SrsMetaStore {
        String current;
        final List<String> advances = new ArrayList<>();

        @Override
        public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
            current = sourceReadOffset;
            advances.add(sourceReadOffset);
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

    /** A cdc port replaying a fixed list of change events to the listener. */
    private static final class FakeCdcPort implements CapturePort {
        private final List<Envelope> events;

        FakeCdcPort(List<Envelope> events) {
            this.events = events;
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            for (Envelope e : events) {
                listener.onEvent(e);
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
