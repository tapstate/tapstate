package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsDurableFrontier;
import io.tapstate.core.event.ChainPosition;
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
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsMetaStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The durable-frontier proof for the sink-ack loop: as the sink confirms writes, the pipeline's durable
 * {@code sinkAckedSrcpos} advances in the SRS meta, so the source-read frontier has a real input. It runs
 * the whole assembly end to end — cdc capture fills the ring, the store-backed topology reads it, a real
 * transform runs, and the sink advances the durable watermark — over one embedded Jet + SRS member.
 *
 * <p>Changes are fed one at a time and each is awaited at the sink before the next, so a change lands in
 * its own sink batch. How far the durable ack advances is set by the two proofs a position can be closed
 * on: a strictly higher position settling, and a bound covering it once nothing is left in flight. With
 * positions {@code src-0..src-3} fed one per batch, the first proof carries the prefix to {@code src-2},
 * and the source's own bound closes {@code src-3} behind it — so the durable prefix reaches the last change
 * fed rather than trailing one behind it.
 *
 * <p>The value asserted here is what says the bound is being acted on at all, and it is deliberately not
 * what it used to be. Until 2026-09-01 bounds were discarded on this shape and this assertion read
 * {@code src-2}, one behind. That was safe and it was also not enough: a full load has no second change to
 * close its snapshot rows with, every row of one snapshot carrying the same reserved position, so a
 * frontier that could only close a position by settling a higher one left every table it had written
 * unrecorded — and a resume read all of them again. A job that stopped acting on bounds would put this
 * assertion back to {@code src-2}, which is the only way that regression is visible.
 *
 * <p>Scope: {@code cdc_only}, so the snapshot phase does not run.
 */
class CaptureToSinkAckFrontierTest {

    private static final String PIPELINE = "p";

    /** The peer that reads the same source directly -- the one this file did not have. */
    private static final String DIRECT_PIPELINE = "p_direct";
    private static final String SOURCE_ID = "orders_src";
    private static final String DEST_ID = "orders_dest";
    private static final String TABLE = "orders";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("capture-to-sink-ack-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        member = Hazelcast.newHazelcastInstance(config);
        CapturingSinkWriter.reset();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("the sink advances the pipeline's durable sinkAckedSrcpos as it confirms writes")
    void sinkConfirmsAdvanceTheDurableSinkAckedSourcePosition() {
        InMemoryStorePort store = seedStore();
        GatedSource gatedSource = new GatedSource();
        LifecycleActuator actuator = wireRuntime(store, gatedSource, UnaryOperator.identity());

        SrsMetaStore meta = store.meta();
        String chainId = SourceCaptureResolution
                .of(StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID)).chainId().value();

        actuator.start(PIPELINE);
        try {
            // Feed four cdc changes one at a time, each awaited at the sink so it lands in its own batch:
            // positions src-0, src-1, src-2, src-3 in feed order -- the source states each one. Every settled
            // batch is reaped, on the next input while they arrive and on the idle hook once the tail goes
            // quiet. src-0..src-2 are closed by the change above each of them; src-3 has nothing above it and
            // is closed by the source's bound instead, once no write is left in flight.
            gatedSource.feed(change(0));
            awaitSinkSize(1);
            gatedSource.feed(change(1));
            awaitSinkSize(2);
            gatedSource.feed(change(2));
            awaitSinkSize(3);
            gatedSource.feed(change(3));
            awaitSinkSize(4);

            awaitSinkAck(meta, chainId, "src-3");
            // Closed by the bound rather than by a higher change: nothing higher was ever fed.
            assertThat(ackedPosition(meta, chainId)).isEqualTo("src-3");

            // The observation position resolver reads back exactly that durable sink-acked position, keyed by
            // the source's table, so the read face projects what the real sink advanced -- not a stand-in.
            assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE))
                    .containsExactly(entry(TABLE, "src-3"));
        } finally {
            actuator.stop(PIPELINE, true);
        }

        assertThat(gatedSource.cdcClosed).as("stop closes the capture subscription").isTrue();
    }

    @Test
    @DisplayName("the source vertex the product path assembles announces how far the frontier may go")
    void theAssembledJobCarriesABoundOffItsSource() {
        BoundProbe.reset();
        InMemoryStorePort store = seedStore();
        GatedSource gatedSource = new GatedSource();
        // The graph is the product one; the probe only listens on a spare ordinal of the source vertex the
        // product path built. Nothing about which chain that source stamps, or which axis it travels on, is
        // supplied by the test - that is the wiring under test.
        LifecycleActuator actuator = wireRuntime(store, gatedSource, CaptureToSinkAckFrontierTest::withBoundProbe);
        String chainId = SourceCaptureResolution
                .of(StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID)).chainId().value();

        actuator.start(PIPELINE);
        long epoch;
        try {
            gatedSource.feed(change(0));
            awaitSinkSize(1);
            epoch = store.meta().read(chainId).orElseThrow().epoch();
            awaitBound();
        } finally {
            actuator.stop(PIPELINE, true);
        }

        // One chain in this job, so it is numbered onto the first axis after the one the engine keeps for its
        // own markers; the bound stands for the change that was read, which is the whole of what a source can
        // promise. A job assembled without a frontier binding announces nothing here at all.
        assertThat(BoundProbe.seen())
                .containsExactly("1:" + FrontierOrders.pack(TABLE, new SourceOrder(epoch, 0)));
    }

    /**
     * The product topology with a listener hung off a spare outbound ordinal of its source vertex. A DAG is
     * still being built by the product path - this only adds somewhere for what the source broadcasts to be
     * observed, which no sink writer can see because a bound is not a record.
     */
    private static DagSource withBoundProbe(DagSource product) {
        return new DagSource() {

            /** Delegated whole, so the probe changes the topology and nothing else about the run. */
            @Override
            public NestCapacity capacityOf(String pipelineId) {
                return product.capacityOf(pipelineId);
            }

            @Override
            public DAG dagFor(String pipelineId) {
                DAG dag = product.dagFor(pipelineId);
                Vertex probe = dag.newVertex("bound_probe", ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of(BoundProbe::new)));
                dag.edge(Edge.from(dag.getVertex(SOURCE_ID), 1).to(probe));
                return dag;
            }

            @Override
            public java.util.List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(
                    String pipelineId) {
                // The probe adds a vertex, never state: whatever the product keeps is all there is to name.
                return product.stateHeldBy(pipelineId);
            }
        };
    }

    private void awaitBound() {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (BoundProbe.seen().isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the assembled job's source announced no bound at all");
            }
            park();
        }
    }

    /** Records every bound broadcast to it as {@code axis:value}; the changes it is also handed go no further. */
    private static final class BoundProbe extends AbstractProcessor {

        private static final Queue<String> SEEN = new ConcurrentLinkedQueue<>();

        static Queue<String> seen() {
            return SEEN;
        }

        static void reset() {
            SEEN.clear();
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            SEEN.add(watermark.key() + ":" + watermark.timestamp());
            return true;
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            return true;
        }
    }

    /** The source, the sink connection and a passthrough-filter pipeline over one cdc-only table. */
    private static InMemoryStorePort seedStore() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE_ID, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(TABLE)), null, null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));
        // A passthrough filter (keeps every change), so every fed position reaches the sink and the sink size
        // tracks the number of changes fed. cdc_only, so only the change tail runs.
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE_ID, true)),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemas().save(new DiscoveredSourceModel(SOURCE_ID, "fake", 0L, new SourceModel(List.of(
                new SourceTable(TABLE, List.of(new SourceField("id", "INT")), List.of("id"), List.of())))));
        return store;
    }

    /**
     * The runtime around the seeded store: the member made SRS- and sink-capable as the assembly root does,
     * the real capture coordinator, the real store-backed topology, and a sink bound to a capturing writer.
     * {@code wrapDag} is what a test puts between the topology and the engine when it needs to watch the
     * graph the product path built - the graph itself is still the product one.
     */
    private LifecycleActuator wireRuntime(
            InMemoryStorePort store, GatedSource gatedSource, UnaryOperator<DagSource> wrapDag) {
        SrsMetaStore meta = store.meta();
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("not resolved by this ack test");
        };
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);

        SnapshotBuffer snapshotBuffer = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshotBuffer);

        SrsCoordinator srsCoordinator = new SrsCoordinator(meta);
        CaptureRunUnit captureRunUnit = new CaptureRunUnit(gatedSource, srsCoordinator, meta, member);
        PipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, captureRunUnit::start, srsCoordinator, snapshotBuffer);

        StoreBackedDagSource.SinkWriterBinder capturingSink =
                (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) CapturingSinkWriter::new;
        DagSource dagSource = wrapDag.apply(new StoreBackedDagSource(store, capturingSink));
        return new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));
    }

    /**
     * A pipeline reading its source directly still counts when the chain works out what it may forget.
     *
     * <p>Turning the shared buffer off changes where a pipeline reads from. It must not change whether the
     * chain knows it is there: the frontier is the slowest consumer's acked position, and a consumer the
     * frontier cannot see is one it will pass -- taking the record past changes that pipeline has not
     * landed, which is the loss that leaves nothing behind to find.
     *
     * <p>Two consumers, and that is not decoration. With one, a frontier that has lost sight of it takes a
     * minimum over nothing and returns nothing at all -- the same answer as a frontier that is correctly
     * held back, so the case would pass either way. It takes a second, faster consumer for "the slow one
     * was ignored" and "the slow one held it" to be different values.
     *
     * <p>The faster one is moved ahead by hand rather than by racing it. What is under test is whether the
     * direct pipeline is in the set the minimum is taken over, and a race that happened to leave them level
     * would make the two readings equal again -- passing, and saying nothing.
     *
     * <p>No mutation stands behind this one, and the reason is worth more than a mutation would be: there
     * is no branch to break. What puts a pipeline in that set is the acknowledgement wiring, and the wiring
     * walks the pipeline's sources and resolves each one's chain without ever reading the buffering switch.
     * A direct pipeline is not admitted by a special case; it is admitted because nothing asks. So this is
     * a guard against that distinction being introduced rather than a witness of it being absent -- and it
     * is not a vacuous one: it reddens the day anyone adds the branch, which is the day it would matter.
     */
    @Test
    @DisplayName("a directly-read pipeline is still one of the consumers the frontier waits for")
    void aDirectlyReadPipelineStillCountsInTheChainsFrontier() {
        InMemoryStorePort store = seedStoreWithADirectPeer();
        GatedSource gatedSource = new GatedSource();
        LifecycleActuator actuator = wireRuntime(store, gatedSource, UnaryOperator.identity());

        SrsMetaStore meta = store.meta();
        String chainId = SourceCaptureResolution
                .of(StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID)).chainId().value();

        actuator.start(PIPELINE);
        actuator.start(DIRECT_PIPELINE);
        try {
            gatedSource.feed(change(0));
            awaitSinkSize(2);
            awaitConsumers(meta, chainId, 2);

            assertThat(meta.consumerOffsets(chainId))
                    .as("the consumers the chain knows about: the switch decides where a pipeline reads "
                            + "from, not whether the chain can see it")
                    .extracting(ConsumerOffset::pipelineId)
                    .containsExactlyInAnyOrder(PIPELINE, DIRECT_PIPELINE);

            ChainPosition acked = directAckedPosition(meta, chainId);
            assertThat(acked).as("the direct pipeline's own acked position").isNotNull();

            // The other one races ahead. By hand, because what is under test is membership of the set the
            // minimum is taken over -- and a race that left them level would make both readings equal.
            ChainPosition farAhead = new ChainPosition(
                    new SourceOrder(acked.order().epoch(), acked.order().seq() + 1_000), "src-far");
            meta.advanceSinkAcked(chainId, PIPELINE, farAhead);

            assertThat(SrsDurableFrontier.safeAdvance(farAhead, meta.consumerOffsets(chainId)))
                    .as("what the chain may forget once one consumer has run far ahead: the direct one is "
                            + "still in the minimum, so the answer is where it got to and not where the "
                            + "fast one did. Left out of the set, this is the fast one's position and the "
                            + "record moves past changes the direct pipeline has not landed")
                    .contains(acked);
        } finally {
            actuator.stop(DIRECT_PIPELINE, true);
            actuator.stop(PIPELINE, true);
        }
    }

    /** The direct pipeline's acked position, or null while it has acked nothing. */
    private static ChainPosition directAckedPosition(SrsMetaStore meta, String chainId) {
        return meta.consumerOffsets(chainId).stream()
                .filter(offset -> DIRECT_PIPELINE.equals(offset.pipelineId()))
                .map(ConsumerOffset::sinkAcked)
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private void awaitConsumers(SrsMetaStore meta, String chainId, int expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (meta.consumerOffsets(chainId).size() < expected
                || directAckedPosition(meta, chainId) == null) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + expected
                        + " consumers with the direct one having acked; the chain knew "
                        + meta.consumerOffsets(chainId));
            }
            park();
        }
    }

    /**
     * The seeded store with a second pipeline over the same source, reading it directly.
     *
     * <p>The same source declaration for both, which is what puts them on one chain: what a chain is keyed
     * on is the connection, and the buffering switch sits on the pipeline's reference to the source rather
     * than on the source.
     */
    private static InMemoryStorePort seedStoreWithADirectPeer() {
        InMemoryStorePort store = seedStore();
        store.artifacts().save(new PipelineResource(DIRECT_PIPELINE, null,
                List.of(SourceRef.spec(SOURCE_ID, false)),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        return store;
    }

    private static Envelope change(int id) {
        return Envelope.insert(id, TABLE, Map.of("id", (long) id), Map.of());
    }

    private void awaitSinkSize(int size) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (CapturingSinkWriter.collected().size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + size + " changes at the sink, got "
                        + CapturingSinkWriter.collected().size() + ": " + CapturingSinkWriter.collected());
            }
            park();
        }
    }

    private void awaitSinkAck(SrsMetaStore meta, String chainId, String expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!expected.equals(ackedPosition(meta, chainId))) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for durable sinkAckedSrcpos=" + expected
                        + ", last observed=" + ackedPosition(meta, chainId)
                        + ", collected srcPos=" + CapturingSinkWriter.collected());
            }
            park();
        }
    }

    private static String ackedPosition(SrsMetaStore meta, String chainId) {
        return meta.read(chainId).map(record -> record.consumerOffsets().stream()
                .filter(offset -> offset.pipelineId().equals(PIPELINE))
                .map(ConsumerOffset::sinkAckedSrcpos)
                .findFirst()
                .orElse(null)).orElse(null);
    }

    private static void park() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while polling", e);
        }
    }

    /**
     * A fake connector whose cdc stream is driven on demand: {@code cdc} starts a daemon that emits each fed
     * change to the listener, so the test can release changes one at a time while the pipeline runs live.
     */
    private static final class GatedSource implements CapturePort {

        private final LinkedBlockingQueue<Envelope> pending = new LinkedBlockingQueue<>();
        private volatile boolean running;
        private volatile boolean cdcClosed;
        private Thread daemon;

        void feed(Envelope change) {
            pending.add(change);
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch();
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            running = true;
            daemon = new Thread(() -> {
                while (running) {
                    try {
                        Envelope change = pending.poll(25, TimeUnit.MILLISECONDS);
                        if (change != null) {
                            listener.onBatch(java.util.List.of(change), java.util.Optional.of(new SourcePosition("src-" + change.ts())));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "gated-source-cdc");
            daemon.setDaemon(true);
            daemon.start();
            return () -> {
                running = false;
                cdcClosed = true;
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

    /** An empty snapshot batch: cdc_only never drains one, but the port contract requires the method. */
    private static final class FakeBatch implements CaptureBatch {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Envelope next() {
            throw new java.util.NoSuchElementException();
        }

        @Override
        public Optional<SourcePosition> seam() {
            // Sampled by the source before its first row. The run refuses to start a tail without one,
            // because a tail that begins wherever it likes loses every change made while the snapshot ran.
            return Optional.of(new SourcePosition("seam-0"));
        }

        @Override
        public void close() {
        }
    }

    /** Records each event's src position into a JVM-static queue, shared with the test thread on one member. */
    private static final class CapturingSinkWriter implements SinkWriter {

        private static final Queue<String> COLLECTED = new ConcurrentLinkedQueue<>();

        static Queue<String> collected() {
            return COLLECTED;
        }

        static void reset() {
            COLLECTED.clear();
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                COLLECTED.add(record.position() == null ? null : record.position().token());
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
