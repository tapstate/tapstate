package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromRef;
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
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.SrsMetaStore;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The data-flow proof for one pipeline started end to end: a real cdc capture fills the per-table change
 * ring, the store-backed topology reads that ring, a real transform runs over the stream, and a sink
 * receives the result - cdc -&gt; ring -&gt; Jet source -&gt; transform -&gt; sink - all driven through the
 * real {@code EngineLifecycleActuator}, over one embedded Jet + SRS member.
 *
 * <p>Two seams keep it connector-free while exercising the real assembly: the capture side drives a fake
 * {@link CapturePort} (no PDK connector), and the topology's sink is bound to a capturing writer through the
 * builder's sink-writer seam (no PDK sink). Everything between - the source-vertex ring-name derivation, the
 * transform chain, the verb composition, and critically that the capture and the reader derive the same ring
 * from the same source - is the production code. Data arriving at the sink is the proof the two agree on the
 * ring; a mismatch would leave the sink empty.
 *
 * <p>Scope: {@code cdc_only}, so the snapshot phase does not run; routing the snapshot through the transform
 * chain is a later increment, and the coordinator drains snapshot rows to a no-op pass-through until then.
 */
class CaptureToSinkDataFlowTest {

    private static final String PIPELINE = "p";
    private static final String SOURCE_ID = "orders_src";
    private static final String DEST_ID = "orders_dest";
    private static final String TABLE = "orders";
    private static final String SINK = "capture-to-sink-flow";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member -- never merge with anything on the LAN.
        config.setClusterName("capture-to-sink-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        // The Jet execution engine is on: the built topology runs inside a Jet job over the change ring.
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
    @DisplayName("start flows captured changes through the ring, source, transform and into the sink; stop tears the capture down")
    void capturedChangesFlowThroughTheTransformIntoTheSinkAndStopTearsCaptureDown() {
        // Seed the store: a cdc source (one table, srs default-on), a sink connection supplier, and a pipeline
        // that filters to even ids and serves the result to the sink. cdc_only so only the change tail runs.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE_ID, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(TABLE)), null, null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SOURCE_ID),
                List.of(Step.inline("keep_even", io.tapstate.core.model.FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("after.id % 2 == 0"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_even"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);

        // Make the member SRS-capable and sink-capable, exactly as the assembly root does: bind the meta store
        // and a connector provisioner into the user context. The provisioner is never resolved here (the fake
        // capture and the capturing sink bypass it), but binding it is the sink-capable step under test.
        SrsMetaStore meta = store.meta();
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("not resolved by this data-flow test");
        };
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);

        // Make the member snapshot-capable too: the same buffer the coordinator fills is bound member-side so a
        // source drains it ahead of the tail. cdc_only here, so the snapshot phase never runs and the buffer
        // stays empty -- the source is a pure tail, unchanged.
        SnapshotBuffer snapshotBuffer = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshotBuffer);

        // The capture side drives a fake connector feeding four changes (ids 0..3) into the cdc stream.
        FakeSource fakeSource = new FakeSource(List.of(change(0), change(1), change(2), change(3)));
        SrsCoordinator srsCoordinator = new SrsCoordinator(meta);
        CaptureRunUnit captureRunUnit = new CaptureRunUnit(fakeSource, srsCoordinator, meta, member);
        PipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, captureRunUnit::start, srsCoordinator, snapshotBuffer);

        // The real store-backed topology, with the sink bound to a capturing writer so the run needs no PDK
        // sink connector -- the source-vertex ring-name derivation and the transform chain are the real thing.
        StoreBackedDagSource.SinkWriterBinder capturingSink =
                (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) CapturingSinkWriter::new;
        DagSource dagSource = new StoreBackedDagSource(store, capturingSink);

        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, new NestStateTeardown(member, store.keyedState()));

        actuator.start(PIPELINE);
        try {
            awaitSinkSize(2);
        } finally {
            actuator.stop(PIPELINE);
        }

        // The sink saw exactly the even-id changes the filter kept, after the transform, in read order, each
        // carrying the injected stream name and the source position the projection lifted from the ring item.
        // Their arrival proves the whole path AND that the capture and the reader derived the same ring.
        assertThat(CapturingSinkWriter.collected())
                .containsExactly(TABLE + "|w1|0", TABLE + "|w3|2");

        // Stop tore the capture down: the fake source's subscription was closed (the cdc daemon is gone, no
        // leak), the pipeline's Jet job is terminal, and the coordinator dropped the handle.
        assertThat(fakeSource.cdcClosed).as("stop closes the capture subscription").isTrue();
        Job job = member.getJet().getJob(PIPELINE);
        assertThat(job).isNotNull();
        awaitTerminal(job); // cancel is asynchronous, so poll for terminal rather than racing the check
        assertThat(((StoreBackedPipelineCaptureCoordinator) coordinator).isActive(PIPELINE))
                .as("stop drops the capture handle").isFalse();
    }

    private static Envelope change(int id) {
        // The id is carried as a long so the filter's CEL modulo (int64-only) has a matching overload; a real
        // connector's numeric fields decode to the codec's own type, which the transform layer owns.
        return Envelope.insert(id, TABLE, Map.of("id", (long) id), Map.of());
    }

    private static void awaitTerminal(Job job) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (job.getStatus().isTerminal()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting terminal job", e);
            }
        }
        throw new AssertionError("stop did not cancel the pipeline's job within budget; last status was "
                + job.getStatus());
    }

    private void awaitSinkSize(int size) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (CapturingSinkWriter.collected().size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "timed out waiting for " + size + " changes at the sink, got "
                                + CapturingSinkWriter.collected().size() + ": " + CapturingSinkWriter.collected());
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting sink", e);
            }
        }
    }

    /** A fake connector: a fixed change stream driven into the listener synchronously when cdc starts. */
    private static final class FakeSource implements CapturePort {

        private final List<Envelope> changes;
        volatile boolean cdcClosed;

        FakeSource(List<Envelope> changes) {
            this.changes = changes;
        }

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new FakeBatch(List.of());
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureListener listener) {
            for (Envelope change : changes) {
                listener.onEvent(change);
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
     * A sink writer that records each event as a stable {@code src|srcPos|id} string into a JVM-static queue,
     * so the embedded run can assert what the built DAG delivered. The writer is opened member-side behind the
     * sink seam; on the single embedded member the static queue is shared with the test thread. Completes each
     * write synchronously.
     */
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
                COLLECTED.add(record.src() + "|" + (record.position() == null ? null : record.position().token()) + "|" + record.after().get("id"));
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
