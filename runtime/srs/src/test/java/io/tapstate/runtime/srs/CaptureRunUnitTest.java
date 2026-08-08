package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.ReadMode;
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The capture run unit assembles the snapshot phase, cdc phase, the self-built Jet ring source and the
 * mining-chain coordinator into one source run, dispatched by the pipeline's consumption plan (its read
 * mode and its {@code srs.enabled} flag). It runs over a single embedded Hazelcast member sized to the L1
 * hot-buffer shape (capacity 8): a real per-table change ring for the shared-ring cdc paths, and a mock
 * connector (a fixed snapshot batch and a fixed change stream) standing in for a real PDK source.
 */
class CaptureRunUnitTest {

    private static HazelcastInstance hz;

    /** Orders the mock positions {@code w1 < w2 < ...} by numeric suffix; a source position is never ordered lexically. */
    private static final Comparator<String> NUMERIC_ORDER = Comparator.comparingInt(p -> Integer.parseInt(p.substring(1)));

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-rununit-test-" + System.nanoTime());
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
        return new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders"));
    }

    private static Envelope row(int id) {
        return Envelope.read(id, "orders", Map.of("id", id), Map.of());
    }

    private static Envelope change(int id) {
        return Envelope.insert(id, "orders", Map.of("id", id), Map.of());
    }

    /** A mock cdc watermark: a monotonic source-position generator (w1, w2, ...) standing in for the connector position. */
    private static Supplier<SourcePosition> monotonicWatermark() {
        AtomicLong n = new AtomicLong();
        return () -> new SourcePosition("w" + n.incrementAndGet());
    }

    /** A run spec with the mock L1 collaborators filled in, config-derived chain (no srs.key). */
    private static CaptureRunSpec spec(ReadMode mode, boolean srsEnabled) {
        return spec(mode, srsEnabled, null);
    }

    /**
     * A run spec keyed by an explicit {@code srsKey} so each shared-ring test gets its own mining chain —
     * and so its own per-table ring on the member shared across this class, keeping the tests isolated.
     */
    private static CaptureRunSpec spec(ReadMode mode, boolean srsEnabled, String srsKey) {
        return new CaptureRunSpec(
                config(), mode, srsKey, srsEnabled, "src-1", "pipe-1", StartFrom.earliest(),
                new SourcePosition("cdc-start-0"), null, 0L, monotonicWatermark());
    }

    private CaptureRunUnit runUnit(CapturePort port, SrsMetaStore meta) {
        return new CaptureRunUnit(port, new SrsCoordinator(meta), meta, hz);
    }

    @Test
    void snapshotOnlyDrainsToThePassthroughWithNoChainNoCdcStartAndNoTail() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_ONLY, true), passthrough::add);

        // snapshot_only is a bounded pass straight to the sink: no shared chain a cdc tail resumes against,
        // so nothing is provisioned, no cdc-start is recorded, and no tail is attached.
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(1, 2, 3);
        assertThat(run.snapshotCount()).isEqualTo(3);
        assertThat(run.chainId()).isEmpty();
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isEmpty();
        assertThat(meta.created).isEmpty();
        assertThat(port.cdcStarted).isFalse();
    }

    @Test
    void snapshotAndCdcOverASharedRingProvisionsSnapshotsAttachesAndWritesTheChangeRing() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2)), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.SNAPSHOT_AND_CDC, true, "chain-snap-cdc"), passthrough::add);

        // The full source run: the chain is provisioned and seeded, the snapshot drains straight to the sink
        // (recording the cdc-start position at the seam), the consumer attaches, and the cdc tail writes the
        // shared change ring the exposed Jet source reads.
        assertThat(run.chainId()).isPresent();
        assertThat(run.merged()).isFalse();
        assertThat(run.snapshotCount()).isEqualTo(2);
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(1, 2);
        assertThat(run.ringSource()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(port.cdcStarted).isTrue();

        String chainId = run.chainId().get().value();
        assertThat(meta.created).containsExactly(chainId);
        assertThat(meta.read(chainId)).get().extracting(SrsMeta::cdcStartPosition).isEqualTo("cdc-start-0");

        Ringbuffer<SrsItem> ring = hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders"));
        assertThat(ring.tailSequence()).isEqualTo(1L);
        assertThat(ring.readOne(0).after()).containsEntry("id", 10);
        assertThat(ring.readOne(1).after()).containsEntry("id", 11);
    }

    @Test
    void cdcOnlyOverASharedRingSkipsTheSnapshotButStillProvisionsAndWritesTheRing() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1)), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, true, "chain-cdc-only"), passthrough::add);

        // cdc_only skips the initial snapshot: nothing drains to the sink and no cdc-start position is
        // recorded (there is no snapshot seam), but the chain is still provisioned and the tail writes the ring.
        assertThat(run.snapshotCount()).isEqualTo(0);
        assertThat(passthrough).isEmpty();
        assertThat(run.chainId()).isPresent();
        assertThat(run.ringSource()).isPresent();
        assertThat(run.cdcSubscription()).isPresent();

        String chainId = run.chainId().get().value();
        assertThat(meta.read(chainId)).get().extracting(SrsMeta::cdcStartPosition).isNull();
        Ringbuffer<SrsItem> ring = hz.getRingbuffer(SrsRingbuffer.ringName(chainId, "orders"));
        assertThat(ring.tailSequence()).isEqualTo(1L);
    }

    @Test
    void srsDisabledStreamsTheTailStraightToThePassthroughWithNoChainOrRing() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(), List.of(change(10), change(11)));
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false), passthrough::add);

        // srs.enabled:false is the lightweight direct path: the cdc tail streams straight to the single
        // consumer with no shared ring, no coordinator chain, and no durable meta.
        assertThat(passthrough).extracting(e -> e.after().get("id")).containsExactly(10, 11);
        assertThat(run.chainId()).isEmpty();
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isPresent();
        assertThat(port.cdcStarted).isTrue();
        assertThat(meta.created).isEmpty();
    }

    @Test
    void srsDisabledSurfacesADeadTailAsAFailureOnTheRun() {
        InMemoryMeta meta = new InMemoryMeta();
        RuntimeException boom = new RuntimeException("tail boom");
        FakeSource port = new FakeSource(List.of(), List.of()).failing(boom);
        List<Envelope> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(spec(ReadMode.CDC_ONLY, false), passthrough::add);

        // The direct tail reported a failure; the run surfaces it so a coordinator polling the run can see a
        // dead tail rather than a run that merely stopped emitting.
        assertThat(run.failure()).contains(boom);
    }

    @Test
    void surfacesTheForceMergeWhenASecondSourceResolvesToTheSameChain() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureRunUnit unit = new CaptureRunUnit(
                new FakeSource(List.of(), List.of()), new SrsCoordinator(meta), meta, hz);

        CaptureRun first = unit.start(spec(ReadMode.CDC_ONLY, true, "chain-merge"), e -> { });
        CaptureRun second = unit.start(spec(ReadMode.CDC_ONLY, true, "chain-merge"), e -> { });

        // The first source opens the chain; a second source resolving to the same chain force-merges onto it
        // rather than mining the source twice -- the signal a caller surfaces as a shared capture.
        assertThat(first.merged()).isFalse();
        assertThat(second.merged()).isTrue();
    }

    @Test
    void rejectsAMultiTableSharedRingRunAsBeyondSingleTableScope() {
        InMemoryMeta meta = new InMemoryMeta();
        CaptureConfig multi = new CaptureConfig("mysql", Map.of(), List.of("orders", "customers"));
        CaptureRunSpec spec = new CaptureRunSpec(multi, ReadMode.CDC_ONLY, "k-multi", true, "src-1", "pipe-1",
                StartFrom.earliest(), new SourcePosition("cdc-start-0"), null, 0L, monotonicWatermark());

        // L1 capture is single-table: a multi-table shared-ring run is rejected before any chain is opened.
        assertThatThrownBy(() -> runUnit(new FakeSource(List.of(), List.of()), meta).start(spec, e -> { }))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(meta.created).isEmpty();
    }

    @Test
    void theReadCursorPublisherResolvesTheStoreMemberSideAndAdvancesTheConsumerCursor() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-pub", null);
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        try {
            SrsReadCursorPublisherFactory factory =
                    CaptureRunUnit.readCursorPublisher("chain-pub", "pipe-7", "orders");

            factory.resolve(hz).accept(7L);

            // The factory holds only coordinates; resolved on the member it binds the store from the user
            // context and advances exactly this consumer's per-table cursor.
            ConsumerOffset offset = meta.read("chain-pub").orElseThrow().consumerOffsets().stream()
                    .filter(c -> c.pipelineId().equals("pipe-7")).findFirst().orElseThrow();
            assertThat(offset.perTableSeq()).containsEntry("orders", 7L);
        } finally {
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void theReadCursorPublisherResolvesToANoOpWhenNoStoreIsBoundOnTheMember() {
        hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);

        // No store bound on the member: the factory resolves to a no-op sink, so a source still runs before
        // the assembly layer makes the member SRS-capable. Resolving and calling it does not throw.
        SrsReadCursorPublisherFactory factory = CaptureRunUnit.readCursorPublisher("chain-x", "pipe-x", "orders");
        factory.resolve(hz).accept(3L);
    }

    @Test
    void minConsumerReadSeqIsTheSlowestCursorAcrossTheChainsConsumers() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-min", null);
        // Two consumers on the chain: one has read orders up to 5, the other only to 2 -- the slowest bounds it.
        meta.advanceConsumerReadSeq("chain-min", "p1", "orders", 5L);
        meta.advanceConsumerReadSeq("chain-min", "p2", "orders", 2L);

        assertThat(CaptureRunUnit.minConsumerReadSeq(meta, "chain-min", "orders")).isEqualTo(2L);
    }

    @Test
    void minConsumerReadSeqIsUnconstrainedWhenNoConsumerHasACursorYet() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-none", null);

        // No consumer has published a cursor: nothing constrains the ring, so the write gate sees no bound.
        assertThat(CaptureRunUnit.minConsumerReadSeq(meta, "chain-none", "orders")).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void minConsumerReadSeqTreatsAConsumerThatHasNotReadTheTableAsHavingReadNothing() {
        InMemoryMeta meta = new InMemoryMeta();
        meta.create("chain-other", null);
        // The consumer has a cursor on another table but none on orders: for orders it has read nothing (-1),
        // holding the orders ring at the head until it starts reading orders.
        meta.advanceConsumerReadSeq("chain-other", "p1", "customers", 9L);

        assertThat(CaptureRunUnit.minConsumerReadSeq(meta, "chain-other", "orders")).isEqualTo(-1L);
    }

    /** A mock connector: a fixed snapshot batch and a fixed change stream driven into the listener when cdc starts. */
    private static final class FakeSource implements CapturePort {
        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;
        private Throwable cdcError;
        boolean cdcStarted;
        boolean cdcClosed;

        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes) {
            this.snapshotRows = snapshotRows;
            this.changes = changes;
        }

        /** Makes this source's cdc stream report a failure through the listener rather than deliver changes. */
        FakeSource failing(Throwable error) {
            this.cdcError = error;
            return this;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch(snapshotRows);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
            cdcStarted = true;
            if (cdcError != null) {
                listener.onError(cdcError);
                return () -> cdcClosed = true;
            }
            for (Envelope e : changes) {
                listener.onEvent(e);
            }
            return () -> cdcClosed = true;
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

    /** A bounded snapshot batch over a fixed list of events. */
    private static final class FakeBatch implements CaptureBatch {
        private final Iterator<Envelope> events;

        FakeBatch(List<Envelope> events) {
            this.events = events.iterator();
        }

        @Override
        public boolean hasNext() {
            return events.hasNext();
        }

        @Override
        public Envelope next() {
            return events.next();
        }

        @Override
        public void close() {
        }
    }

    /**
     * A faithful in-memory {@link SrsMetaStore}: insert-only create, per-facet mutators that reject an
     * unseeded chain, and a read-cursor advance that upserts one consumer's {@code perTableSeq} without
     * clobbering its sink-ack — enough to exercise the run unit's provision, cdc-start, offset and cursor
     * wiring without a store backend.
     */
    private static final class InMemoryMeta implements SrsMetaStore {
        final List<String> created = new ArrayList<>();
        private final Map<String, SrsMeta> records = new LinkedHashMap<>();

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public void create(String miningChainId, String retention) {
            if (records.containsKey(miningChainId)) {
                throw new IllegalStateException("mining chain already seeded: " + miningChainId);
            }
            created.add(miningChainId);
            records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), sourceReadOffset, m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
            next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
            next.add(offset);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset c : m.consumerOffsets()) {
                if (c.pipelineId().equals(pipelineId)) {
                    existing = c;
                } else {
                    next.add(c);
                }
            }
            Map<String, Long> perTable = new LinkedHashMap<>(existing == null ? Map.of() : existing.perTableSeq());
            perTable.put(table, lastReadSeq);
            ChainPosition ack = existing == null ? null : existing.sinkAcked();
            next.add(new ConsumerOffset(pipelineId, perTable, ack));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            ConsumerOffset existing = null;
            for (ConsumerOffset c : m.consumerOffsets()) {
                if (c.pipelineId().equals(pipelineId)) {
                    existing = c;
                } else {
                    next.add(c);
                }
            }
            Map<String, Long> perTable = existing == null ? Map.of() : existing.perTableSeq();
            next.add(new ConsumerOffset(pipelineId, perTable, position));
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), cdcStartPosition,
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), snapshotEpoch));
        }

        @Override
        public long openEpoch(String miningChainId) {
            SrsMeta m = require(miningChainId);
            long opened = m.epoch() + 1;
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), opened, m.snapshotEpoch()));
            return opened;
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            SrsMeta m = require(miningChainId);
            List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
            next.add(version);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), m.cdcStartPosition(),
                    next, m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String table) {
            SrsMeta m = require(miningChainId);
            if (m.snapshotCompletedTables().contains(table)) {
                return;
            }
            List<String> next = new ArrayList<>(m.snapshotCompletedTables());
            next.add(table);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), next, m.epoch(), m.snapshotEpoch()));
        }

        private SrsMeta require(String miningChainId) {
            SrsMeta m = records.get(miningChainId);
            if (m == null) {
                throw new IllegalStateException("mining chain not seeded: " + miningChainId);
            }
            return m;
        }
    }
}
