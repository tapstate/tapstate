package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import io.tapstate.core.event.Envelope;
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

import java.time.Duration;
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

/**
 * The data-plane smoke for the assembled capture run: the run unit driven over a single embedded
 * Jet-enabled, SRS-capable member with a single-table mock connector, exercising all three read modes
 * end to end. The two ring-bearing modes stream the change ring through a real Jet job over the self-built
 * {@link SrsRingSource}, and the pass-through (snapshot) and the Jet sink converge on one downstream list
 * so it witnesses the whole snapshot -> cdc stream in order and count. snapshot_only exposes no ring source,
 * so there is no downstream Jet stream at all. The two ring modes also witness the durable read cursor
 * advancing member-side as the source drains — the store bound in the member user context, resolved and
 * written through the running job, not called directly.
 *
 * <p>Where {@link CaptureRunUnitTest} asserts the assembly's ring writes on a Jet-disabled member, this
 * runs the exposed Jet source inside a real job and observes the downstream. The mock watermark and
 * position order stand in for real connector machinery; the sink-ack backpressure loop is out of scope here.
 */
class CaptureRunUnitJetSmokeTest {

    private static HazelcastInstance hz;

    /** Orders the mock positions {@code w1 < w2 < ...} by numeric suffix; a source position is never ordered lexically. */
    private static final Comparator<String> NUMERIC_ORDER = Comparator.comparingInt(p -> Integer.parseInt(p.substring(1)));

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("srs-smoke-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        // The Jet execution engine is on: the exposed ring source runs inside a Jet job.
        config.getJetConfig().setEnabled(true);
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
    void snapshotAndCdcDeliversTheContinuousSnapshotThenCdcStreamToADownstreamJetStage() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of(change(10), change(11)));
        String downName = "smoke-down-snapcdc";
        IList<Object> downstream = hz.getList(downName);
        downstream.clear();

        // The member is SRS-capable: bind this run's store under the user-context key the ring source's
        // reader resolves member-side to publish its read cursor -- the assembly layer's binding, in the test.
        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        Job job = null;
        try {
            // Snapshot drains straight to the pass-through (op=r, bypassing the ring); the cdc tail writes the
            // change ring; the returned Jet source tails it. The pass-through and the Jet sink converge on one
            // downstream list.
            CaptureRun run = runUnit(port, meta).start(
                    spec(ReadMode.SNAPSHOT_AND_CDC, "chain-snapcdc"), env -> downstream.add(env.after().get("id")));

            assertThat(run.snapshotCount()).isEqualTo(3);
            assertThat(run.ringSource()).isPresent();

            Pipeline p = Pipeline.create();
            // Pinned to local parallelism one = the production topology (the ring source is a single reader
            // and the sink runs total-parallelism-one): a default-parallelism map or list sink would fan the
            // single ordered change stream across racing instances and the observed list order would no longer
            // be the read order.
            p.readFrom(run.ringSource().orElseThrow()).withoutTimestamps()
                    .map(item -> item.after().get("id")).setLocalParallelism(1)
                    .writeTo(Sinks.list(downName)).setLocalParallelism(1);
            job = hz.getJet().newJob(p);
            awaitSize(downstream, 5);

            // One continuous stream at the downstream: the three snapshot rows (drained first) then the two cdc
            // changes (streamed through the Jet ring source), in order, count matching the source.
            assertThat(downstream).containsExactly(1, 2, 3, 10, 11);

            // The durable read cursor advanced through the running Jet job: the source's member-side reader
            // resolved the store from the user context and published its last read sequence (1, the 2nd change).
            assertThat(perTableSeq(meta, run, "pipe-1", "orders")).isEqualTo(1L);
        } finally {
            if (job != null) {
                job.cancel();
            }
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void cdcOnlyStreamsTheChangeRingToADownstreamJetStageSkippingTheSnapshot() throws Exception {
        InMemoryMeta meta = new InMemoryMeta();
        // A snapshot batch is available but cdc_only must not read it.
        FakeSource port = new FakeSource(List.of(row(1)), List.of(change(10), change(11), change(12)));
        String downName = "smoke-down-cdconly";
        IList<Object> downstream = hz.getList(downName);
        downstream.clear();

        hz.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        Job job = null;
        try {
            List<Object> passthrough = new ArrayList<>();
            CaptureRun run = runUnit(port, meta).start(
                    spec(ReadMode.CDC_ONLY, "chain-cdconly"), env -> passthrough.add(env.after().get("id")));

            // cdc_only skips the snapshot entirely: nothing drains to the pass-through even though a batch was
            // available, and the tail writes the change ring the Jet source tails.
            assertThat(passthrough).isEmpty();
            assertThat(run.snapshotCount()).isEqualTo(0);
            assertThat(run.ringSource()).isPresent();

            Pipeline p = Pipeline.create();
            // Pinned to local parallelism one = the production topology (the ring source is a single reader
            // and the sink runs total-parallelism-one): a default-parallelism map or list sink would fan the
            // single ordered change stream across racing instances and the observed list order would no longer
            // be the read order.
            p.readFrom(run.ringSource().orElseThrow()).withoutTimestamps()
                    .map(item -> item.after().get("id")).setLocalParallelism(1)
                    .writeTo(Sinks.list(downName)).setLocalParallelism(1);
            job = hz.getJet().newJob(p);
            awaitSize(downstream, 3);

            assertThat(downstream).containsExactly(10, 11, 12);
            // Three changes drained (seqs 0..2): the reader published 2 as its last read sequence.
            assertThat(perTableSeq(meta, run, "pipe-1", "orders")).isEqualTo(2L);
        } finally {
            if (job != null) {
                job.cancel();
            }
            hz.getUserContext().remove(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        }
    }

    @Test
    void snapshotOnlyDrainsToThePassthroughAndExposesNoJetRingStream() {
        InMemoryMeta meta = new InMemoryMeta();
        FakeSource port = new FakeSource(List.of(row(1), row(2), row(3)), List.of());
        List<Object> passthrough = new ArrayList<>();

        CaptureRun run = runUnit(port, meta).start(
                spec(ReadMode.SNAPSHOT_ONLY, "chain-snaponly"), env -> passthrough.add(env.after().get("id")));

        // snapshot_only is a one-shot pass straight to the sink with no incremental tail: the rows drain to the
        // pass-through, and there is no ring source for a downstream Jet stage to read -- nothing to stream, no
        // shared chain opened.
        assertThat(passthrough).containsExactly(1, 2, 3);
        assertThat(run.snapshotCount()).isEqualTo(3);
        assertThat(run.ringSource()).isEmpty();
        assertThat(run.cdcSubscription()).isEmpty();
        assertThat(run.chainId()).isEmpty();
        assertThat(meta.created).isEmpty();
    }

    private CaptureRunUnit runUnit(CapturePort port, SrsMetaStore meta) {
        return new CaptureRunUnit(port, new SrsCoordinator(meta), meta, hz);
    }

    /** A run spec keyed by an explicit {@code srsKey} so each shared-ring test gets its own chain and per-table ring. */
    private static CaptureRunSpec spec(ReadMode mode, String srsKey) {
        return new CaptureRunSpec(
                config(), mode, srsKey, true, "src-1", "pipe-1", StartFrom.earliest(),
                new SourcePosition("cdc-start-0"), null, 0L, monotonicWatermark());
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

    /** This consumer's last read sequence on {@code table}, read back from the chain's durable consumer offsets. */
    private static long perTableSeq(SrsMetaStore meta, CaptureRun run, String pipelineId, String table) {
        String chainId = run.chainId().orElseThrow().value();
        return meta.read(chainId).orElseThrow().consumerOffsets().stream()
                .filter(c -> c.pipelineId().equals(pipelineId)).findFirst().orElseThrow()
                .perTableSeq().getOrDefault(table, -1L);
    }

    private static void awaitSize(IList<?> list, int size) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (list.size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + size + " changes, got " + list.size());
            }
            Thread.sleep(50);
        }
    }

    /** A mock connector: a fixed snapshot batch and a fixed change stream driven into the listener when cdc starts. */
    private static final class FakeSource implements CapturePort {
        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;
        boolean cdcStarted;
        boolean cdcClosed;

        FakeSource(List<Envelope> snapshotRows, List<Envelope> changes) {
            this.snapshotRows = snapshotRows;
            this.changes = changes;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch(snapshotRows);
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
            cdcStarted = true;
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
     * A faithful in-memory {@link SrsMetaStore}, synchronized so the Jet worker's member-side cursor advance is
     * visible to the test thread's read-back: insert-only create, per-facet mutators that reject an unseeded
     * chain, and a read-cursor advance that upserts one consumer's {@code perTableSeq} without clobbering its
     * sink-ack — enough to exercise the run unit's provision, cdc-start, offset and cursor wiring without a
     * store backend.
     */
    private static final class InMemoryMeta implements SrsMetaStore {
        final List<String> created = new ArrayList<>();
        private final Map<String, SrsMeta> records = new LinkedHashMap<>();

        @Override
        public synchronized Optional<SrsMeta> read(String miningChainId) {
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public synchronized void create(String miningChainId, String retention) {
            if (records.containsKey(miningChainId)) {
                throw new IllegalStateException("mining chain already seeded: " + miningChainId);
            }
            created.add(miningChainId);
            records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
        }

        @Override
        public synchronized void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), sourceReadOffset, m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            SrsMeta m = require(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
            next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
            next.add(offset);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), next, m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public synchronized void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
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
        public synchronized void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
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
        public synchronized void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            SrsMeta m = require(miningChainId);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), cdcStartPosition,
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), m.epoch(), snapshotEpoch));
        }

        @Override
        public synchronized long openEpoch(String miningChainId) {
            SrsMeta m = require(miningChainId);
            long opened = m.epoch() + 1;
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), m.cdcStartPosition(),
                    m.schemaHistory(), m.retention(), m.snapshotCompletedTables(), opened, m.snapshotEpoch()));
            return opened;
        }

        @Override
        public synchronized void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            SrsMeta m = require(miningChainId);
            List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
            next.add(version);
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceReadOffset(), m.consumerOffsets(), m.cdcStartPosition(),
                    next, m.retention(), m.snapshotCompletedTables(), m.epoch(), m.snapshotEpoch()));
        }

        @Override
        public synchronized void markSnapshotComplete(String miningChainId, String table) {
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
