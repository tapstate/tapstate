package io.tapstate.runtime.srs;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The core-API projected source over a per-table change ring: a {@code ProcessorMetaSupplier} that tails the
 * ring and emits each change already projected to the transform-facing {@link Envelope} currency - the shape
 * the engine's DAG builder wires as a source vertex. Where {@link SrsRingSourceTest} proves the pipeline-API
 * source over raw {@link SrsItem}s, this proves the core-API vertex that injects the stream name and carries
 * the source position into the envelope, runs inside a real Jet job, stays live as a tail, honours
 * backpressure without loss, and publishes its read cursor member-side. Runs over one embedded Jet-enabled
 * member (ring capacity 8).
 */
class SrsSourceProcessorTest {

    private static final String CURSOR_KEY = "test.source.cursor";

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        config.setClusterName("srs-source-p-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
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
    void streams_changes_projected_to_envelopes_carrying_the_stream_name_and_source_position()
            throws InterruptedException {
        fill("srs.chain.orders", 5);

        // source -> project(Envelope to an assertable string) -> list. The projection is what a raw-SrsItem
        // source could not do: it injects the stream name and lifts the position token into the envelope.
        List<String> out = runProjectedToStrings("srs.chain.orders", "orders", "out-orders", 5, 1024);

        assertThat(out).containsExactly(
                "orders|w0|0|1:0", "orders|w1|1|1:1", "orders|w2|2|1:2", "orders|w3|3|1:3", "orders|w4|4|1:4");
    }

    @Test
    void loses_no_change_when_the_downstream_backpressures() throws InterruptedException {
        fill("srs.chain.bp", 6);

        // A one-deep edge queue forces the source's outbox to reject mid-fill, so the source must buffer the
        // unemitted remainder and finish it on a later run rather than advancing past it. Every change still
        // arrives, in order - the proof the projected source honours Jet backpressure without dropping a read.
        List<String> out = runProjectedToStrings("srs.chain.bp", "orders", "out-bp", 6, 1);

        assertThat(out).containsExactly(
                "orders|w0|0|1:0", "orders|w1|1|1:1", "orders|w2|2|1:2", "orders|w3|3|1:3", "orders|w4|4|1:4", "orders|w5|5|1:5");
    }

    @Test
    void keeps_tailing_the_ring_after_it_has_drained_the_initial_backlog() throws InterruptedException {
        fill("srs.chain.live", 3);

        // A change ring is a live tail, not a bounded read: once the initial backlog drains the source must
        // stay running and pick up changes appended afterwards. If it completed on catching up, an append made
        // while the job runs would never be tailed - fatal for a source whose offset truth is not in Jet state.
        Job job = hz.getJet().newJob(projectedDag(
                "srs.chain.live", "orders", "out-live", 1024, SrsReadCursorPublisherFactory.NONE));
        IList<String> out = hz.getList("out-live");
        try {
            awaitSize(out, 3);
            assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);

            // Append after the job is already running: a live tail must deliver it.
            new SrsRingbuffer(hz.getRingbuffer("srs.chain.live"))
                    .append(new SrsItem(new SourcePosition("w3"), Op.INSERT, 1L, null, Map.of("id", 3), 0L));
            awaitSize(out, 4);

            assertThat(out).containsExactly(
                    "orders|w0|0|1:0", "orders|w1|1|1:1", "orders|w2|2|1:2", "orders|w3|3|1:3");
        } finally {
            job.cancel();
        }
    }

    @Test
    void publishes_the_read_cursor_member_side_as_it_drains() throws InterruptedException {
        PUBLISHED.clear();
        fill("srs.chain.cursor", 5);

        // The factory must resolve its sink off the member the source runs on: it is bound into the member's
        // user context and read back through the member argument, so a source that resolved with a wrong or
        // null instance would fail here rather than silently reporting nothing.
        hz.getUserContext().put(CURSOR_KEY, (LongConsumer) SrsSourceProcessorTest::collect);
        SrsReadCursorPublisherFactory factory =
                member -> (LongConsumer) member.getUserContext().get(CURSOR_KEY);
        Job job = hz.getJet().newJob(projectedDag("srs.chain.cursor", "orders", "out-cursor", 1024, factory));
        IList<String> out = hz.getList("out-cursor");
        try {
            awaitSize(out, 5);
        } finally {
            job.cancel();
            hz.getUserContext().remove(CURSOR_KEY);
        }

        // The source reports its read progress member-side: the last sequence it read (4, the 5th change).
        assertThat(PUBLISHED).isNotEmpty();
        assertThat(PUBLISHED.get(PUBLISHED.size() - 1)).isEqualTo(4L);
    }

    @Test
    void emits_all_buffered_snapshot_rows_before_it_tails_the_ring() throws InterruptedException {
        // A snapshot buffer bound member-side carries the source's snapshot rows; the ring holds the cdc tail.
        // The source must drain the whole buffer first, then tail the ring -- snapshot (op r, no position)
        // strictly before cdc (op i, positioned), the ordering that keeps a stale snapshot from landing after
        // a newer change. Distinct id ranges make the two streams unmistakable in the observed order.
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append("srs.chain.snapfirst", snapshotRow(100));
        buffer.append("srs.chain.snapfirst", snapshotRow(101));
        buffer.append("srs.chain.snapfirst", snapshotRow(102));
        fill("srs.chain.snapfirst", 2);
        hz.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);

        List<String> out;
        try {
            out = runProjectedToStrings("srs.chain.snapfirst", "orders", "out-snapfirst", 5, 1024);
        } finally {
            hz.getUserContext().remove(SnapshotBuffer.USER_CONTEXT_KEY);
        }

        // The three snapshot rows (no source position) come first, in buffered order, then the two cdc changes.
        assertThat(out).containsExactly(
                "orders|null|100|null", "orders|null|101|null", "orders|null|102|null", "orders|w0|0|1:0", "orders|w1|1|1:1");
    }

    @Test
    void everyChangeOfAGenerationOutranksEverySnapshotRowOfIt() throws InterruptedException {
        // The snapshot phase stamps its rows with the generation the snapshot began in before they reach the
        // buffer; the ring's changes take the same generation and the sequence the ring assigned them.
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append("srs.chain.inv1", snapshotRow(100).withOrder(SourceOrder.snapshotRow(1L)));
        buffer.append("srs.chain.inv1", snapshotRow(101).withOrder(SourceOrder.snapshotRow(1L)));
        fill("srs.chain.inv1", 2);
        hz.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);

        List<String> out;
        try {
            out = runProjectedToStrings("srs.chain.inv1", "orders", "out-inv1", 4, 1024);
        } finally {
            hz.getUserContext().remove(SnapshotBuffer.USER_CONTEXT_KEY);
        }

        // This is what makes a snapshot safe to replay against a stream that has already moved on: the two
        // are ordered against each other, and every change of the generation wins. Asserting only that the
        // rows arrive first would pass on a source that emits them in order but leaves them unordered --
        // and a stateful node reorders on the order, not on arrival.
        assertThat(out).containsExactly(
                "orders|null|100|1:" + SourceOrder.SNAPSHOT_SEQ,
                "orders|null|101|1:" + SourceOrder.SNAPSHOT_SEQ,
                "orders|w0|0|1:0",
                "orders|w1|1|1:1");
    }

    @Test
    void staysLiveOnceItsRowsAreOutAndItsRingIsOneNobodyFills() throws InterruptedException {
        // A source reading no chain of its own - its rows come from the snapshot buffer and its ring is one
        // nobody fills - has nothing left to do the moment the buffer is drained. It must keep running
        // anyway, because a source finishing is what lets every vertex behind it finish in turn, and a
        // vertex finishing is the one event that raises a bound above what an instance promised: with no
        // queue left to hold the bound down, the engine offers the highest value any of them ever reported.
        // A frontier handed that carries over whatever a finishing vertex was still holding and had never
        // sent, and nothing anywhere reports it. Nothing else in this vertex enforces that, so a bounded
        // read added later - the very shape this one stands in for - would take the property away silently.
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append("srs.chain.nofinish", snapshotRow(100));
        buffer.append("srs.chain.nofinish", snapshotRow(101));
        hz.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);

        Job job = hz.getJet().newJob(projectedDag(
                "srs.chain.nofinish", "orders", "out-nofinish", 1024, SrsReadCursorPublisherFactory.NONE, 0L));
        try {
            awaitSize(hz.getList("out-nofinish"), 2);
            Thread.sleep(500);

            assertThat(job.getStatus())
                    .describedAs("the source finished once it had nothing left to read, which finishes every "
                            + "vertex behind it and hands the sink a bound nobody stands behind")
                    .isEqualTo(JobStatus.RUNNING);
        } finally {
            job.cancel();
            hz.getUserContext().remove(SnapshotBuffer.USER_CONTEXT_KEY);
        }
    }

    @Test
    void refusesAChangeFoundOnARingWhoseChainHasNoGenerationOpen() throws InterruptedException {
        // A source with no generation reads no chain of its own, so its ring is one nobody fills. A change
        // sitting on it means a capture is writing a chain that was never opened -- and the alternative to
        // refusing it is putting every change of this job below every real generation, which reorders data
        // silently and no assertion on the rows would catch.
        fill("srs.chain.nogen", 2);

        Job job = hz.getJet().newJob(new DAG().vertex(new Vertex("source",
                SrsSourceProcessor.metaSupplier("srs.chain.nogen", "orders", StartFrom.earliest(), 0L,
                        SrsReadCursorPublisherFactory.NONE))));

        assertThatThrownBy(() -> job.join())
                .hasMessageContaining("srs.chain.nogen")
                .hasMessageContaining("no ring generation open");
    }

    @Test
    void tails_the_ring_unchanged_when_no_snapshot_buffer_is_bound() throws InterruptedException {
        // The cdc-only path: no buffer bound, so the source behaves exactly as before -- pure ring tail.
        fill("srs.chain.nobuffer", 3);

        List<String> out = runProjectedToStrings("srs.chain.nobuffer", "orders", "out-nobuffer", 3, 1024);

        assertThat(out).containsExactly("orders|w0|0|1:0", "orders|w1|1|1:1", "orders|w2|2|1:2");
    }

    @Test
    void stampsTheFrontierAtTheLastChangeItHasEmitted() throws InterruptedException {
        SEEN.clear();
        fill("srs.chain.bound", 3);

        // A source is the only vertex that can say a change exists at all, so the frontier starts here. What a
        // bound is encoded as belongs to whoever wires the job -- this ring knows a sequence, not an axis or a
        // packing -- so the test stamps its own: axis 7, the sequence itself.
        runRecordingBounds("srs.chain.bound", "orders", "out-bound", 3, 1024, "b:7:2");

        // The bound trails the changes it stands for. A source that spoke first would be promising changes
        // still sitting in its own outbox, and nothing later takes such a promise back.
        assertThat(SEEN).containsSubsequence("i:0", "i:1", "i:2", "b:7:2");
    }

    @Test
    void neverPromisesAChangeStillWaitingInItsOwnBuffer() throws InterruptedException {
        SEEN.clear();
        fill("srs.chain.boundbp", 6);

        // A one-deep edge queue makes the outbox refuse mid-batch, so the source finishes emitting the read
        // remainder on a later run. A bound worked out while part of that batch is still buffered would be
        // claiming changes that have not left this vertex, and no later message takes such a claim back.
        runRecordingBounds("srs.chain.boundbp", "orders", "out-boundbp", 6, 1, "b:7:5");

        assertThat(SEEN).containsSubsequence("i:5", "b:7:5");
    }

    @Test
    void saysNothingFurtherWhileTheRingStaysWhereItWas() throws InterruptedException {
        SEEN.clear();
        fill("srs.chain.boundidle", 2);

        // A source polls its ring continuously, so "nothing new" is the common case. The engine fails a job
        // outright on a bound that does not climb - equal counts - which makes a periodic repeat of the last
        // bound fatal rather than merely wasteful. Silence is the only correct thing to say.
        Job job = hz.getJet().newJob(recordingDag("srs.chain.boundidle", "orders", "out-boundidle", 1024));
        try {
            awaitSize(hz.getList("out-boundidle"), 2);
            Thread.sleep(500);

            assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);
            assertThat(SEEN.stream().filter(entry -> entry.startsWith("b:")).toList())
                    .containsExactly("b:7:1");
        } finally {
            job.cancel();
        }
    }

    @Test
    void pins_the_source_vertex_to_a_single_instance_across_the_cluster() throws Exception {
        // One reader per ring is what keeps the change stream in order; a per-member instance would re-lane it.
        // A static resolution check: a total-parallelism-one supplier hands the real supplier to one member and
        // a no-op to the rest, so resolving over several members yields more than one distinct supplier.
        ProcessorMetaSupplier meta = SrsSourceProcessor.metaSupplier(
                "srs.chain.pins", "orders", StartFrom.earliest(), 1L, SrsReadCursorPublisherFactory.NONE);
        List<Address> addresses = List.of(
                Address.createUnresolvedAddress("10.0.0.1", 5701),
                Address.createUnresolvedAddress("10.0.0.2", 5702),
                Address.createUnresolvedAddress("10.0.0.3", 5703));
        meta.init(new TestProcessorMetaSupplierContext().setTotalParallelism(3).setLocalParallelism(1));
        Function<? super Address, ? extends ProcessorSupplier> assignment = meta.get(addresses);

        assertThat(addresses.stream().map(assignment).distinct().count()).isGreaterThan(1);
    }

    /**
     * Runs source -> record -> list with a bounded edge queue, waiting until {@code size} changes arrive.
     * Everything the recording vertex is handed, changes and bounds alike, lands in {@link #SEEN} in the
     * order it arrived - which is what "a bound never overtakes the changes it covers" is a claim about.
     */
    private static void runRecordingBounds(String ringName, String src, String sinkName, int size, int queueSize,
            String bound) throws InterruptedException {
        Job job = hz.getJet().newJob(recordingDag(ringName, src, sinkName, queueSize));
        try {
            awaitSize(hz.getList(sinkName), size);
            awaitSeen(bound);
        } finally {
            job.cancel();
        }
    }

    /**
     * Waits until {@code entry} has been recorded. The changes reach the sink one vertex further on than the
     * bound is recorded at, so their arrival says nothing about whether the bound has been handed over yet;
     * waiting on the bound itself is what makes the order it arrived in a fact rather than a race.
     */
    private static void awaitSeen(String entry) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!SEEN.contains(entry)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + entry + ", saw " + SEEN);
            }
            Thread.sleep(50);
        }
    }

    /**
     * source -> record -> list, with the source stamping its read progress. The stamp is the test's own -
     * axis 7, the sequence itself - because a change ring knows a sequence and nothing about which axis its
     * stream was numbered onto or how a bound is packed.
     */
    private static DAG recordingDag(String ringName, String src, String sinkName, int queueSize) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SrsSourceProcessor.metaSupplier(
                ringName, src, StartFrom.earliest(), 1L, SrsReadCursorPublisherFactory.NONE,
                order -> new Watermark(order.seq(), (byte) 7)));
        Vertex record = dag.newVertex("record", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of(RecordingBounds::new)));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(sinkName)).localParallelism(1);
        dag.edge(between(source, record).setConfig(new EdgeConfig().setQueueSize(queueSize)))
                .edge(between(record, sink));
        return dag;
    }

    /** What one vertex was handed, in arrival order: {@code i:<id>} for a change, {@code b:<axis>:<bound>}. */
    private static final List<String> SEEN = new CopyOnWriteArrayList<>();

    /** Records every change and every bound that reaches it, passing the changes on so the sink can be awaited. */
    private static final class RecordingBounds extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            Envelope event = (Envelope) item;
            SEEN.add("i:" + event.after().get("id"));
            return tryEmit(String.valueOf(event.after().get("id")));
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            SEEN.add("b:" + watermark.key() + ":" + watermark.timestamp());
            return true;
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            return true;
        }
    }

    /** Runs source -> project-to-string -> list with a bounded edge queue, waiting until {@code size} arrive. */
    private static List<String> runProjectedToStrings(
            String ringName, String src, String sinkName, int size, int queueSize) throws InterruptedException {
        Job job = hz.getJet().newJob(
                projectedDag(ringName, src, sinkName, queueSize, SrsReadCursorPublisherFactory.NONE));
        IList<String> out = hz.getList(sinkName);
        try {
            awaitSize(out, size);
        } finally {
            job.cancel();
        }
        return List.copyOf(out);
    }

    /**
     * source -> project-to-string -> list, every stage at parallelism one as the production pipeline is (source,
     * transform and sink are each pinned to one instance): a default-parallelism map or list sink would fan the
     * single ordered stream across racing instances and the observed list order would no longer be the read order.
     */
    private static DAG projectedDag(String ringName, String src, String sinkName, int queueSize,
            SrsReadCursorPublisherFactory publisherFactory) {
        return projectedDag(ringName, src, sinkName, queueSize, publisherFactory, 1L);
    }

    /** The same graph over a source opened under {@code epoch}; zero is a source reading no chain of its own. */
    private static DAG projectedDag(String ringName, String src, String sinkName, int queueSize,
            SrsReadCursorPublisherFactory publisherFactory, long epoch) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source",
                SrsSourceProcessor.metaSupplier(ringName, src, StartFrom.earliest(), epoch, publisherFactory));
        Vertex project = dag.newVertex("project", Processors.mapP(SrsSourceProcessorTest::describe))
                .localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(sinkName)).localParallelism(1);
        dag.edge(between(source, project).setConfig(new EdgeConfig().setQueueSize(queueSize)))
                .edge(between(project, sink));
        return dag;
    }

    /** A stable, Hazelcast-serializable projection of an envelope: {@code src|token|id|epoch:seq}. */
    private static String describe(Envelope event) {
        ChainPosition at = event.position();
        return event.src() + "|" + (at == null ? null : at.token()) + "|" + event.after().get("id")
                + "|" + order(at);
    }

    /** A position's order rendered {@code epoch:seq}, or {@code null} where there is none. */
    private static String order(ChainPosition at) {
        return at == null || at.order() == null ? "null" : at.order().epoch() + ":" + at.order().seq();
    }

    /** A snapshot read envelope (op r, no source position) on the {@code orders} stream. */
    private static Envelope snapshotRow(int id) {
        return Envelope.read(1L, "orders", Map.of("id", id), Map.of());
    }

    /** Pre-fills a ring with {@code count} inserts at sequences {@code 0..count-1}, positions {@code w0..}. */
    private static void fill(String ringName, int count) {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        for (int i = 0; i < count; i++) {
            ring.append(new SrsItem(new SourcePosition("w" + i), Op.INSERT, 1L, null, Map.of("id", i), 0L));
        }
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

    private static final List<Long> PUBLISHED = new CopyOnWriteArrayList<>();

    private static void collect(long lastReadSeq) {
        PUBLISHED.add(lastReadSeq);
    }
}
