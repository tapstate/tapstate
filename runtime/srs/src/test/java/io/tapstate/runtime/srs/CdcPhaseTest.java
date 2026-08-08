package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cdc phase drives an unbounded change stream through the headroom gate into the per-table change
 * ring: each change event (ops {@code i} / {@code u} / {@code d} / {@code ddl}) is projected to a ring
 * item, admitted only while a consumer's unread change is not at risk, and the source read offset is
 * advanced — clamped to the slowest consumer's sink-acked position — as writes land. The ring runs over a
 * single embedded Hazelcast member sized to the L1 hot-buffer shape (capacity 8), matching the write-gate
 * tests.
 *
 * <p>The per-event source position is threaded here at the cdc seam: the event envelope carries no
 * position slot, so at L1 a mock monotonic watermark supplies each change its opaque position (D10); real
 * per-event position threading from the connector is a later concern.
 */
class CdcPhaseTest {

    private static HazelcastInstance hz;

    /** Orders the mock positions {@code w1 < w2 < w3 < ...} by their numeric suffix; a source position is never ordered lexically. */
    /** The generation the ring under test is running under; every order on it carries this first. */
    private static final long RING_GENERATION = 1L;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-cdc-test-" + System.nanoTime());
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

    private static CaptureConfig config() {
        return new CaptureConfig("mysql", Map.of(), List.of("orders"));
    }

    /** A mock cdc watermark: a monotonic source-position generator (w1, w2, ...) standing in for the connector-defined per-event position. */
    private static Supplier<SourcePosition> monotonicWatermark() {
        AtomicLong n = new AtomicLong();
        return () -> new SourcePosition("w" + n.incrementAndGet());
    }

    /** A change-ring item used to fill the ring in the backpressure test; a plain insert at the given position. */
    private static SrsItem cdcItem(String token) {
        return new SrsItem(new SourcePosition(token), Op.INSERT, 1L, null, Map.of("id", 1), 0L);
    }

    /** Polls {@code thread}'s state until it reaches {@code target} or the timeout elapses; the final state decides. */
    private static boolean awaitState(Thread thread, Thread.State target, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (thread.getState() == target) {
                return true;
            }
            Thread.sleep(5);
        }
        return thread.getState() == target;
    }

    @Test
    void projectsEachCdcChangeToTheRingInOrder() throws Exception {
        Ringbuffer<SrsItem> ring = hz.getRingbuffer("srs.chain.order");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(ring));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", monotonicWatermark(), RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.update(2, "orders", Map.of("id", 1), Map.of("id", 1, "n", 9), Map.of()),
                Envelope.delete(3, "orders", Map.of("id", 1), Map.of())));

        CdcPhase.run(port, config(), chain, () -> Long.MAX_VALUE, List::of, new CaptureHealth());

        assertThat(ring.tailSequence()).isEqualTo(2L);
        SrsItem first = ring.readOne(0);
        SrsItem second = ring.readOne(1);
        SrsItem third = ring.readOne(2);
        assertThat(first.op()).isEqualTo(Op.INSERT);
        assertThat(first.srcPos()).isEqualTo(new SourcePosition("w1"));
        assertThat(first.after()).containsEntry("id", 1);
        assertThat(second.op()).isEqualTo(Op.UPDATE);
        assertThat(second.srcPos()).isEqualTo(new SourcePosition("w2"));
        assertThat(second.before()).containsEntry("id", 1);
        assertThat(third.op()).isEqualTo(Op.DELETE);
        assertThat(third.srcPos()).isEqualTo(new SourcePosition("w3"));
        assertThat(third.before()).containsEntry("id", 1);
    }

    @Test
    void advancesTheSourceReadOffsetClampedToTheSlowestSinkAckedPosition() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.offset")));
        RecordingMeta meta = new RecordingMeta();
        // One consumer has durably acked its sink up to w2; the persisted read offset must never pass it.
        List<ConsumerOffset> consumers = List.of(new ConsumerOffset("p1", Map.of("orders", 9L), new ChainPosition(new SourceOrder(RING_GENERATION, 1), "w2")));
        CdcChain chain = new CdcChain(gate, meta, "chain", monotonicWatermark(), RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(
                Envelope.insert(1, "orders", Map.of("id", 1), Map.of()),
                Envelope.insert(2, "orders", Map.of("id", 2), Map.of()),
                Envelope.insert(3, "orders", Map.of("id", 3), Map.of())));

        CdcPhase.run(port, config(), chain, () -> Long.MAX_VALUE, () -> consumers, new CaptureHealth());

        // Written at w1, w2, w3; each advance is clamped to the slowest sink-acked position w2, so the
        // persisted offset never passes a change no consumer has durably landed.
        assertThat(meta.advances).containsExactly("w1", "w2", "w2");
    }

    @Test
    void backpressuresARefusedWriteAndRetriesRatherThanDropIt() throws Exception {
        Ringbuffer<SrsItem> raw = hz.getRingbuffer("srs.chain.backpressure");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(raw));
        // Fill the ring to capacity with changes no consumer has read (min read seq -1).
        for (int i = 0; i < 8; i++) {
            assertThat(gate.append(cdcItem("f" + i), -1L)).isPresent();
        }
        // The slowest consumer has read nothing on the first poll (the write is refused), then advances to
        // seq 0 on the next -- modeling the source read pausing until a consumer frees a slot.
        AtomicLong polls = new AtomicLong();
        LongSupplier minRead = () -> polls.getAndIncrement() == 0 ? -1L : 0L;
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", monotonicWatermark(), RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(9, "orders", Map.of("id", 9), Map.of())));

        CdcPhase.run(port, config(), chain, minRead, List::of, new CaptureHealth());

        // The change is not dropped: it lands at seq 8 once headroom frees, and the write was retried
        // (polled more than once) rather than silently overwriting the still-unread seq 0.
        assertThat(raw.tailSequence()).isEqualTo(8L);
        assertThat(raw.readOne(8).srcPos()).isEqualTo(new SourcePosition("w1"));
        assertThat(polls.get()).isGreaterThan(1L);
    }

    @Test
    void parksOffCpuWhileBackpressuredRatherThanBusySpinning() throws Exception {
        Ringbuffer<SrsItem> raw = hz.getRingbuffer("srs.chain.park");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(raw));
        // Fill the ring to capacity with changes no consumer has read, so the next cdc write is refused.
        for (int i = 0; i < 8; i++) {
            assertThat(gate.append(cdcItem("f" + i), -1L)).isPresent();
        }
        // The slowest consumer reads nothing until the test frees a slot: the write stays backpressured.
        AtomicBoolean freed = new AtomicBoolean(false);
        LongSupplier minRead = () -> freed.get() ? 0L : -1L;
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", monotonicWatermark(), RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(9, "orders", Map.of("id", 9), Map.of())));

        Thread writer = new Thread(() -> CdcPhase.run(port, config(), chain, minRead, List::of, new CaptureHealth()), "cdc-writer");
        writer.setDaemon(true);
        try {
            writer.start();
            // A backpressured write parks off-CPU (TIMED_WAITING); it does not burn a core spinning (RUNNABLE).
            boolean parked = awaitState(writer, Thread.State.TIMED_WAITING, Duration.ofSeconds(2));
            // Freeing a slot lets the parked writer wake, land the change and finish -- paused, never dropped.
            freed.set(true);
            writer.join(2000);

            assertThat(parked).isTrue();
            assertThat(writer.isAlive()).isFalse();
            assertThat(raw.tailSequence()).isEqualTo(8L);
            assertThat(raw.readOne(8).srcPos()).isEqualTo(new SourcePosition("w1"));
        } finally {
            freed.set(true);
            writer.interrupt();
        }
    }

    @Test
    void stopsTheStreamThroughTheReturnedSubscription() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.sub")));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", monotonicWatermark(), RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of());

        Subscription sub = CdcPhase.run(port, config(), chain, () -> Long.MAX_VALUE, List::of, new CaptureHealth());
        sub.close();

        // The phase hands back the port's own subscription; closing it stops the stream.
        assertThat(port.closed).isTrue();
    }

    @Test
    void rejectsIncompleteWiringBeforeStartingTheStream() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.guard")));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", monotonicWatermark(), RING_GENERATION, 0L);
        FakeCdcPort port = new FakeCdcPort(List.of(Envelope.insert(1, "orders", Map.of("id", 1), Map.of())));

        assertThatThrownBy(() -> CdcPhase.run(port, config(), chain, null, List::of, new CaptureHealth()))
                .isInstanceOf(NullPointerException.class);

        // Args are validated up front: the stream is never started when the wiring is incomplete.
        assertThat(port.subscribed).isFalse();
    }

    @Test
    void recordsAStreamFailureOnTheHealthSoTheRunCanSurfaceIt() {
        CaptureHealth health = new CaptureHealth();
        RuntimeException boom = new RuntimeException("stream boom");
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.chain.fail")));
        CdcChain chain = new CdcChain(gate, new RecordingMeta(), "chain", monotonicWatermark(), RING_GENERATION, 0L);

        CdcPhase.run(FakeCdcPort.failing(boom), config(), chain, () -> Long.MAX_VALUE, List::of, health);

        // The stream reported a failure rather than a change; the phase records it on the health so the run
        // can surface a dead tail that the change ring merely going quiet would otherwise hide.
        assertThat(health.failure()).contains(boom);
    }

    /** A cdc port that drives a fixed list of change events into the listener when the stream starts. */
    private static final class FakeCdcPort implements CapturePort {
        private final List<Envelope> events;
        private final Throwable error;
        boolean subscribed;
        boolean closed;

        FakeCdcPort(List<Envelope> events) {
            this(events, null);
        }

        private FakeCdcPort(List<Envelope> events, Throwable error) {
            this.events = events;
            this.error = error;
        }

        /** A port whose stream reports a failure instead of delivering changes. */
        static FakeCdcPort failing(Throwable error) {
            return new FakeCdcPort(List.of(), error);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
            subscribed = true;
            if (error != null) {
                listener.onError(error);
                return () -> closed = true;
            }
            for (Envelope e : events) {
                listener.onEvent(e);
            }
            return () -> closed = true;
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

    /** A meta store that records the sequence of source-read-offset advances; the other facets are unused here. */
    private static final class RecordingMeta implements SrsMetaStore {
        final List<String> advances = new ArrayList<>();

        @Override
        public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
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
        public void markSnapshotComplete(String miningChainId, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }
    }
}
