package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
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
import io.tapstate.core.event.ChainPosition;
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
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory;
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
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsConsumerId;
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
    /** Where the pipeline keeps its progress on the chain: under its source node's name. */
    private static final String CONSUMER = SrsConsumerId.of(PIPELINE, SOURCE_ID).value();
    private static final String DEST_ID = "orders_dest";
    private static final String TABLE = "orders";
    /** The second destination, and the sync element writing to it, in the case where one sink holds. */
    private static final String HELD_DEST_ID = "orders_held_dest";
    private static final String HELD_SYNC = "sync_held";

    private HazelcastInstance member;
    private InMemorySrsLogStore log;
    private StoreBackedPipelineCaptureCoordinator coordinator;

    @BeforeEach
    void startMember() {
        log = new InMemorySrsLogStore();
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
                .setBackupCount(0)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setEnabled(true)
                        .setFactoryImplementation(new SrsLogRingbufferStoreFactory(log))));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        member = Hazelcast.newHazelcastInstance(config);
        CapturingSinkWriter.reset();
        HeldSinkWriter.hold();
    }

    @AfterEach
    void stopMember() {
        if (coordinator != null) {
            coordinator.close();
        }
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

            // The same place, said from the chain's side rather than the consumer's: how far it may claim
            // its source has been read. The two are resolved from the same acknowledgements, and this one
            // used to be resolved only while a run of changes was being forwarded -- so the last change
            // fed, whose acknowledgement arrives after the forward that carried it, was never recorded
            // here at all, and the source going quiet is what makes that permanent. A cdc-only read has no
            // recorded start for changes to fall back on, so a run resumed from that state re-attached at
            // the present moment and everything written while it was down was gone, with nothing thrown
            // and nothing logged. Awaited rather than read straight off, because the two are written one
            // after the other and the wait above returns on the first of them.
            awaitSourceRead(meta, chainId, "src-3");

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
    void aFrozenProducerCheckpointReopensAheadOfTheConsumersRetainedChange() {
        InMemoryStorePort store = seedStore();
        GatedSource source = new GatedSource();
        LifecycleActuator first = wireRuntime(store, source, UnaryOperator.identity());
        LifecycleActuator[] current = {first};
        SrsMetaStore meta = store.meta();
        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID));
        String chain = resolution.chainId().value();
        String ring = resolution.ringName(TABLE);
        first.start(PIPELINE);
        try {
            for (int id = 1; id <= 4; id++) {
                source.feed(change(id));
                awaitSinkSize(id);
            }
            awaitSinkAck(meta, chain, "src-4");
            first.pause(PIPELINE);
            var held = member.getJet().getJob(PIPELINE);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (held.getStatus() != com.hazelcast.jet.core.JobStatus.SUSPENDED) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the job did not suspend before the source's fifth change");
                }
                park();
            }
            source.feed(change(5));
            awaitSourceRead(meta, chain, "src-5");
            assertThat(meta.read(chain).orElseThrow().sourceReadDurable()).isTrue();
            assertThat(ackedPosition(meta, chain)).isEqualTo("src-4");
            assertThat(CapturingSinkWriter.collected()).containsExactly("src-1", "src-2", "src-3", "src-4");
            Map<String, Long> confirmed = Map.copyOf(meta.ringDoneThrough(chain, CONSUMER));
            var retained = log.readBatch(ring, 0, 16).records().entrySet().stream()
                    .filter(entry -> "src-5".equals(entry.getValue().srcToken())).toList();
            assertThat(retained).as("the fifth change really exists in the recoverable log").hasSize(1);
            assertThat(retained.getFirst().getValue().after()).containsEntry("id", 5L);
            long retainedSequence = retained.getFirst().getKey();
            var retainedRecord = retained.getFirst().getValue();

            first.stop(PIPELINE, false);
            assertThat(ackedPosition(meta, chain)).isEqualTo("src-4");
            assertThat(meta.ringDoneThrough(chain, CONSUMER)).isEqualTo(confirmed);
            assertThat(log.load(ring, retainedSequence)).contains(retainedRecord);
            int[] beforeSubmission = {0};
            UnaryOperator<CaptureAttacher> frozenReopen = actual -> (spec, handoff, startTail) -> {
                var witness = io.tapstate.spi.store.CaptureResumeWitness.from(spec.sourceId(),
                        spec.config().connectorId(), spec.miningChainId().value(), spec.consumerId(),
                        spec.readMode(), spec.srsEnabled(), spec.config().streams(),
                        meta.read(spec.miningChainId().value()));
                assertThat(witness.sourceReadDurable()).isTrue();
                assertThat(witness.sourceRead().token()).isEqualTo("src-5");
                assertThat(witness.sinkAckedByTable().get(TABLE).token()).isEqualTo("src-4");
                // The existing unfenced test binding exercises the production frozen selection, not HA admission.
                var opened = actual.start(spec.withResumeWitness(witness, null), handoff, startTail);
                try {
                    assertThat(source.starts).hasSize(2);
                    assertThat(source.starts.stream().toList().getLast())
                            .isEqualTo(CaptureStart.resume(new SourcePosition("src-5")));
                    assertThat(ackedPosition(meta, chain)).isEqualTo("src-4");
                    assertThat(meta.ringDoneThrough(chain, CONSUMER)).isEqualTo(confirmed);
                    assertThat(log.load(ring, retainedSequence)).contains(retainedRecord);
                    assertThat(CapturingSinkWriter.collected()).hasSize(4);
                    beforeSubmission[0]++;
                    return opened;
                } catch (RuntimeException | Error failure) {
                    try { opened.close(); }
                    catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
            };
            current[0] = wireRuntime(store, source, UnaryOperator.identity(),
                    (connectorId, settings, mode, ddl, target, node) ->
                            (SupplierEx<SinkWriter>) CapturingSinkWriter::new, frozenReopen);
            current[0].start(PIPELINE);
            assertThat(beforeSubmission[0]).isEqualTo(1);
            awaitSinkSize(5);
            awaitSinkAck(meta, chain, "src-5");
            assertThat(CapturingSinkWriter.collected()).containsExactly("src-1", "src-2", "src-3", "src-4", "src-5");

            assertThat(CapturingSinkWriter.writtenIds()).containsExactly("1", "2", "3", "4", "5");

            source.feed(change(6));
            awaitSinkSize(6);
            awaitSinkAck(meta, chain, "src-6");
            awaitSourceRead(meta, chain, "src-6");
            assertThat(CapturingSinkWriter.collected())
                    .containsExactly("src-1", "src-2", "src-3", "src-4", "src-5", "src-6");
            assertThat(CapturingSinkWriter.writtenIds()).containsExactly("1", "2", "3", "4", "5", "6");
        } finally {
            current[0].stop(PIPELINE, true);
        }
    }

    @Test
    void singleMemberRetiresSinkConfirmedChangesWithoutAnotherPipelineStarting() {
        InMemoryStorePort store = seedStore();
        GatedSource source = new GatedSource();
        LifecycleActuator actuator = wireRuntime(store, source, UnaryOperator.identity());
        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID));
        String chainId = resolution.chainId().value();
        String ring = resolution.ringName(TABLE);

        actuator.start(PIPELINE);
        try {
            source.feed(change(0));
            awaitSinkSize(1);
            source.feed(change(1));
            awaitSinkSize(2);
            awaitSinkAck(store.meta(), chainId, "src-1");

            awaitLogTrimmed(ring, 1L);
            assertThat(log.load(ring, 0L)).isEmpty();
            assertThat(log.load(ring, 1L)).isEmpty();
            assertThat(log.largestSequence(ring)).isEqualTo(1L);

            source.feed(change(2));
            awaitSinkAck(store.meta(), chainId, "src-2");
            awaitLogTrimmed(ring, 2L);
            assertThat(log.load(ring, 2L)).isEmpty();
            assertThat(log.largestSequence(ring)).isEqualTo(2L);
            assertThat(source.starts).hasSize(1);
            assertThat(source.activeSubscriptions()).isEqualTo(1L);
        } finally {
            actuator.stop(PIPELINE, true);
        }
    }

    @Test
    void singleMemberRetainsChangesUntilEveryConsumerConfirms() {
        InMemoryStorePort store = seedStore();
        GatedSource source = new GatedSource();
        LifecycleActuator actuator = wireRuntime(store, source, UnaryOperator.identity());
        SourceCaptureResolution resolution = SourceCaptureResolution.of(
                StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID));
        String chainId = resolution.chainId().value();
        String ring = resolution.ringName(TABLE);
        String slowConsumer = SrsConsumerId.of("paused_peer", SOURCE_ID).value();

        actuator.start(PIPELINE);
        try {
            store.meta().upsertConsumerOffset(chainId, new ConsumerOffset(slowConsumer,
                    Map.of(TABLE, -1L), null, List.of(), null, 0L, Map.of(), ConsumerProgressKind.SRS));
            store.meta().startRingAfter(chainId, slowConsumer, TABLE, -1L);
            source.feed(change(0));
            awaitSinkSize(1);
            source.feed(change(1));
            awaitSinkSize(2);
            awaitSinkAck(store.meta(), chainId, "src-1");

            // The peer's read cursor reaches the second change while its sink confirms only the first.
            long epoch = store.meta().read(chainId).orElseThrow().epoch();
            store.meta().advanceConsumerReadSeq(chainId, slowConsumer, TABLE, 1L);
            store.meta().advanceSinkAcked(chainId, slowConsumer, TABLE,
                    new ChainPosition(new SourceOrder(epoch, 0L), "src-0"));

            awaitLogTrimmed(ring, 0L);
            assertThat(log.load(ring, 0L)).isEmpty();
            assertThat(log.load(ring, 1L)).isPresent();
            assertThat(log.largestSequence(ring)).isEqualTo(1L);

            store.meta().advanceSinkAcked(chainId, slowConsumer, TABLE,
                    new ChainPosition(new SourceOrder(epoch, 1L), "src-1"));
            awaitLogTrimmed(ring, 1L);
            assertThat(log.load(ring, 1L)).isEmpty();
            assertThat(source.starts).hasSize(1);
        } finally {
            actuator.stop(PIPELINE, true);
        }
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
    private InMemoryStorePort seedStore() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE_ID, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(TABLE)), null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null));
        // A passthrough filter (keeps every change), so every fed position reaches the sink and the sink size
        // tracks the number of changes fed. cdc_only, so only the change tail runs.
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE_ID, true)),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts, log);
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
        return wireRuntime(store, gatedSource, wrapDag,
                (connectorId, settings, writeMode, ddl, target, node) -> (SupplierEx<SinkWriter>) CapturingSinkWriter::new);
    }

    /** The same runtime with every sink bound through {@code sinks}. */
    private LifecycleActuator wireRuntime(InMemoryStorePort store, GatedSource gatedSource,
            UnaryOperator<DagSource> wrapDag, StoreBackedDagSource.SinkWriterBinder sinks) {
        return wireRuntime(store, gatedSource, wrapDag, sinks, UnaryOperator.identity());
    }

    /** Adapts only source preparation while retaining the real coordinator, capture unit and native graph. */
    private LifecycleActuator wireRuntime(InMemoryStorePort store, GatedSource gatedSource,
            UnaryOperator<DagSource> wrapDag, StoreBackedDagSource.SinkWriterBinder sinks,
            UnaryOperator<CaptureAttacher> captureStarter) {
        SrsMetaStore meta = store.meta();
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        member.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, store.srsLog());
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("not resolved by this ack test");
        };
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);

        SnapshotBuffer snapshotBuffer = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshotBuffer);

        SrsCoordinator srsCoordinator = new SrsCoordinator(meta);
        CaptureRunUnit captureRunUnit = new CaptureRunUnit(gatedSource, srsCoordinator, meta, member);
        coordinator = new StoreBackedPipelineCaptureCoordinator(
                store, captureStarter.apply(captureRunUnit::start), srsCoordinator, snapshotBuffer);

        DagSource dagSource = wrapDag.apply(new StoreBackedDagSource(store, sinks));
        return new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));
    }

    @Test
    @DisplayName("an independent direct channel resumes without sharing the buffered capture checkpoint")
    void aDirectChannelKeepsItsOwnRecoveryPositionAgainstTheSameDatabase() {
        InMemoryStorePort store = seedStoreWithADirectPeer();
        GatedSource gatedSource = new GatedSource();
        LifecycleActuator actuator = wireRuntime(store, gatedSource, UnaryOperator.identity());
        SrsMetaStore meta = store.meta();
        SourceCaptureResolution physical = SourceCaptureResolution.of(
                StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID));
        String bufferedChain = physical.chainId().value();
        String directChain = physical.scopedTo(DIRECT_PIPELINE, false).chainId().value();
        String bufferedConsumer = SrsConsumerId.of(PIPELINE, SOURCE_ID).value();
        String directConsumer = SrsConsumerId.of(DIRECT_PIPELINE, SOURCE_ID).value();
        assertThat(directChain).isNotEqualTo(bufferedChain);

        actuator.start(PIPELINE);
        actuator.start(DIRECT_PIPELINE);
        try {
            gatedSource.feed(change(0));
            gatedSource.feed(change(1));
            gatedSource.feed(change(2));
            gatedSource.feed(change(3));
            awaitConsumerAck(meta, bufferedChain, bufferedConsumer, "src-3");
            awaitConsumerAck(meta, directChain, directConsumer, "src-3");
            awaitSourceRead(meta, bufferedChain, "src-3");
            awaitSourceRead(meta, directChain, "src-3");
            assertThat(meta.consumerOffsets(bufferedChain)).extracting(ConsumerOffset::pipelineId)
                    .containsExactly(bufferedConsumer);
            assertThat(meta.consumerOffsets(directChain)).extracting(ConsumerOffset::pipelineId)
                    .containsExactly(directConsumer);
            assertThat(CapturingSinkWriter.collected()).containsExactlyInAnyOrder(
                    "src-0", "src-0", "src-1", "src-1", "src-2", "src-2", "src-3", "src-3");

            actuator.stop(DIRECT_PIPELINE, false);
            gatedSource.feed(change(4));
            awaitConsumerAck(meta, bufferedChain, bufferedConsumer, "src-4");
            awaitSourceRead(meta, bufferedChain, "src-4");
            assertThat(sourceRead(meta, directChain)).isEqualTo("src-3");
            assertThat(gatedSource.activeSubscriptions()).isEqualTo(1);

            actuator.start(DIRECT_PIPELINE);
            awaitConsumerAck(meta, directChain, directConsumer, "src-4");
            awaitSourceRead(meta, directChain, "src-4");
            assertThat(gatedSource.starts).contains(CaptureStart.resume(new SourcePosition("src-3")));
            assertThat(meta.consumerOffsets(bufferedChain)).extracting(ConsumerOffset::pipelineId)
                    .containsExactly(bufferedConsumer);
            assertThat(meta.consumerOffsets(directChain)).extracting(ConsumerOffset::pipelineId)
                    .containsExactly(directConsumer);
            assertThat(CapturingSinkWriter.collected()).filteredOn("src-4"::equals).hasSize(2);
        } finally {
            actuator.stop(DIRECT_PIPELINE, true);
            actuator.stop(PIPELINE, true);
        }
    }

    /**
     * A pipeline has landed a change only once every one of its sinks has.
     *
     * <p>Two sinks of one pipeline take the same table's changes, and one of them holds every write it is
     * given. The other writes all four changes and says so. Where the pipeline resumes from is still nowhere
     * at all: a resume from where the faster sink got to would skip every change the held one never wrote,
     * and nothing would ever write them again - the target of the held sink stays short of those rows with
     * the run healthy and nothing logged.
     *
     * <p>What the faster sink said is proven before its absence from the pipeline's record is asserted, so the
     * assertion cannot pass merely because the faster sink had not got round to saying anything yet. The two
     * readings are taken in one loop that stops at whichever comes first: the pipeline's record moving at all
     * is the failure, and the faster sink's own progress arriving is the precondition for the assertion.
     *
     * <p>Released, the held sink writes what it held and the pipeline lands as far as both got - which is
     * the other half of the rule: the slower sink holds the pipeline back only for as long as it is slower.
     */
    @Test
    @DisplayName("a pipeline has not landed a change that one of its sinks still holds")
    void aChangeOneSinkStillHoldsIsNotLandedForThePipeline() {
        InMemoryStorePort store = seedStoreWithTwoSinks();
        GatedSource gatedSource = new GatedSource();
        LifecycleActuator actuator = wireRuntime(store, gatedSource, UnaryOperator.identity(),
                (connectorId, settings, writeMode, ddl, target, node) -> node.nodeId().contains(HELD_SYNC)
                        ? (SupplierEx<SinkWriter>) HeldSinkWriter::new
                        : (SupplierEx<SinkWriter>) CapturingSinkWriter::new);
        SrsMetaStore meta = store.meta();
        String chainId = SourceCaptureResolution
                .of(StoredArtifacts.requireSource(store.artifacts(), SOURCE_ID)).chainId().value();

        actuator.start(PIPELINE);
        try {
            for (int id = 0; id < 4; id++) {
                gatedSource.feed(change(id));
            }
            awaitSinkSize(4);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (ackedPosition(meta, chainId) == null && !anyWriterLanded(meta, chainId, "src-3")) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the sink that wrote every change never said how far it got");
                }
                park();
            }

            assertThat(ackedPosition(meta, chainId))
                    .as("where the pipeline resumes from while one of its sinks has written nothing: a resume "
                            + "from where the other sink got to skips every change the held one holds")
                    .isNull();
            assertThat(meta.ringDoneThrough(chainId, CONSUMER).getOrDefault(TABLE, -1L))
                    .as("how far the pipeline has nothing left to receive from the table's ring")
                    .isNegative();

            HeldSinkWriter.release();
            awaitSinkAck(meta, chainId, "src-3");
            assertThat(meta.ringDoneThrough(chainId, CONSUMER)).containsEntry(TABLE, 3L);
        } finally {
            HeldSinkWriter.release();
            actuator.stop(PIPELINE, true);
        }
    }

    /** Whether any writer of the pipeline's current run has landed {@code token} on the table. */
    private static boolean anyWriterLanded(SrsMetaStore meta, String chainId, String token) {
        return meta.writerRun(chainId, CONSUMER)
                .map(run -> run.progressFor(TABLE).values().stream()
                        .anyMatch(progress -> progress.lastTokened() != null
                                && token.equals(progress.lastTokened().token())))
                .orElse(false);
    }

    /** The seeded pipeline serving its stream to two destinations, one of which will hold its writes. */
    private InMemoryStorePort seedStoreWithTwoSinks() {
        InMemoryStorePort store = seedStore();
        store.artifacts().save(new SourceResource(HELD_DEST_ID, null, "fake", Map.of("host", "e"),
                null, null, null, null));
        store.artifacts().save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE_ID, true)),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null),
                                new SyncElement(HELD_SYNC, HELD_DEST_ID, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        return store;
    }

    private void awaitConsumerAck(SrsMetaStore meta, String chainId, String consumerId, String expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!expected.equals(meta.read(chainId).flatMap(record -> record.consumerOffset(consumerId))
                .map(ConsumerOffset::sinkAckedSrcpos).orElse(null))) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + consumerId + " to confirm " + expected
                        + "; progress=" + meta.consumerOffsets(chainId)
                        + "; sink positions=" + CapturingSinkWriter.collected());
            }
            park();
        }
    }

    /** The same physical database with a second pipeline whose source disables SRS. */
    private InMemoryStorePort seedStoreWithADirectPeer() {
        InMemoryStorePort store = seedStore();
        store.artifacts().save(new PipelineResource(DIRECT_PIPELINE, null,
                List.of(SourceRef.spec(SOURCE_ID, false)),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null)), null, null),
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

    private void awaitLogTrimmed(String ring, long through) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (log.bounds(ring).trimmedThrough() < through) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for durable log retirement through " + through
                        + ", bounds=" + log.bounds(ring));
            }
            park();
        }
        assertThat(log.bounds(ring).trimmedThrough()).isEqualTo(through);
    }

    /** Waits for the chain's own record of how far its source has been read to reach {@code expected}. */
    private void awaitSourceRead(SrsMetaStore meta, String chainId, String expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!expected.equals(sourceRead(meta, chainId))) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for the chain to record its source read at "
                        + expected + ", last observed=" + sourceRead(meta, chainId)
                        + ", this pipeline had acked=" + ackedPosition(meta, chainId));
            }
            park();
        }
    }

    private static String sourceRead(SrsMetaStore meta, String chainId) {
        return meta.read(chainId).map(record -> record.sourceReadOffset()).orElse(null);
    }

    /** The pipeline's acked position, or null while its record holds none - which a started run's record may. */
    private static String ackedPosition(SrsMetaStore meta, String chainId) {
        return meta.read(chainId).map(record -> record.consumerOffsets().stream()
                .filter(offset -> offset.pipelineId().equals(SrsConsumerId.of(PIPELINE, SOURCE_ID).value()))
                .map(ConsumerOffset::sinkAckedSrcpos)
                .filter(java.util.Objects::nonNull)
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

        private final List<Envelope> history = new java.util.ArrayList<>();
        private final List<FakeSubscription> subscriptions = new java.util.ArrayList<>();
        private final Queue<CaptureStart> starts = new ConcurrentLinkedQueue<>();
        private volatile boolean cdcClosed;

        synchronized void feed(Envelope change) {
            history.add(change);
            subscriptions.stream().filter(subscription -> subscription.running)
                    .forEach(subscription -> subscription.pending.add(change));
        }

        synchronized long activeSubscriptions() {
            return subscriptions.stream().filter(subscription -> subscription.running).count();
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch();
        }

        @Override
        public synchronized Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            starts.add(start);
            FakeSubscription subscription = new FakeSubscription(listener);
            long resumeAfter = start instanceof CaptureStart.Resume resume
                    ? Long.parseLong(resume.position().token().substring("src-".length()))
                    : start instanceof CaptureStart.Present && !history.isEmpty()
                            ? history.getLast().ts() : -1L;
            history.stream().filter(change -> change.ts() > resumeAfter)
                    .forEach(subscription.pending::add);
            subscriptions.add(subscription);
            subscription.daemon.start();
            return subscription;
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        private final class FakeSubscription implements Subscription {
            private final LinkedBlockingQueue<Envelope> pending = new LinkedBlockingQueue<>();
            private volatile boolean running = true;
            private final Thread daemon;

            private FakeSubscription(CaptureListener listener) {
                daemon = new Thread(() -> {
                    while (running) {
                        try {
                            Envelope change = pending.poll(25, TimeUnit.MILLISECONDS);
                            if (change != null) {
                                listener.onBatch(List.of(change), Optional.of(new SourcePosition("src-" + change.ts())));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }, "gated-source-cdc");
                daemon.setDaemon(true);
            }

            @Override
            public void close() {
                running = false;
                cdcClosed = true;
                daemon.interrupt();
            }
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

    /**
     * Holds every write it is given until the test releases them all at once: a sink that has written
     * nothing for as long as the test needs it to. JVM-static, like the capturing writer, because the sink
     * opens its writer on the member and the test cannot reach that instance.
     */
    private static final class HeldSinkWriter implements SinkWriter {

        private static volatile CompletableFuture<Void> gate = new CompletableFuture<>();

        static void hold() {
            gate = new CompletableFuture<>();
        }

        static void release() {
            gate.complete(null);
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return gate.thenApply(ignored -> new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** Records each event's src position into a JVM-static queue, shared with the test thread on one member. */
    private static final class CapturingSinkWriter implements SinkWriter {

        private static final Queue<String> COLLECTED = new ConcurrentLinkedQueue<>();
        private static final Queue<String> WRITTEN_IDS = new ConcurrentLinkedQueue<>();

        static Queue<String> collected() {
            return COLLECTED;
        }

        static Queue<String> writtenIds() {
            return WRITTEN_IDS;
        }

        static void reset() {
            COLLECTED.clear();
            WRITTEN_IDS.clear();
        }

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            for (Envelope record : records) {
                COLLECTED.add(record.position() == null ? null : record.position().token());
                Map<String, Object> image = record.after() != null ? record.after() : record.before();
                WRITTEN_IDS.add(image == null ? "null" : String.valueOf(image.get("id")));
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
