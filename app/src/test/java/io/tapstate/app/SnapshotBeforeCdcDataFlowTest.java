package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromClause;
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
import java.util.ArrayList;
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
 * The data-consistency proof for a {@code snapshot_and_cdc} pipeline started end to end: the snapshot rows and
 * the cdc changes flow through the same transform-to-sink chain, and every snapshot row lands at the sink
 * strictly before any cdc change. That ordering is the guarantee under test: a snapshot row carries the older
 * value, so if it reached the sink after a cdc change of the same key the stale value would win. Making the
 * overlap window (cdc replaying changes made during the snapshot) idempotent is a later increment; this proves
 * only the ordering and the routing.
 *
 * <p>Connector-free the same way the cdc-only flow proof is: a fake {@link CapturePort} drives both a bounded
 * snapshot batch and a cdc stream, and the sink is a capturing writer bound through the builder seam. The
 * snapshot buffer, the source-vertex buffer-then-tail emission, the transform chain and the verb composition
 * are the production code, driven through the real {@code EngineLifecycleActuator} over one embedded member.
 */
class SnapshotBeforeCdcDataFlowTest {

    private static final String PIPELINE = "p";
    private static final String SOURCE_ID = "orders_src";
    private static final String DEST_ID = "orders_dest";
    private static final String TABLE = "orders";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("snapshot-before-cdc-test-" + System.nanoTime());
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
    @DisplayName("all snapshot rows reach the sink before any cdc change, through the same transform chain")
    void snapshotRowsArriveBeforeCdcChangesAtTheSink() {
        // A snapshot_and_cdc source: three snapshot rows (ids 100..102) and two cdc changes (ids 0..1), kept
        // apart by id range so the arrival order is unmistakable. A passthrough filter keeps every row so all
        // five reach the sink through the real transform chain.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE_ID, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(TABLE)), null, null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SOURCE_ID),
                List.of(Step.inline("keep_all", FromClause.list(FromRef.literal(SOURCE_ID)),
                        new TransformBody.Filter("true"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_all"),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);

        // Make the member SRS-capable, sink-capable and snapshot-capable, exactly as the assembly root does.
        SrsMetaStore meta = store.meta();
        member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, meta);
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("not resolved by this data-flow test");
        };
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);
        SnapshotBuffer snapshotBuffer = new SnapshotBuffer();
        member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshotBuffer);

        // The fake connector: a bounded snapshot of three rows, then a cdc stream of two changes.
        FakeSource fakeSource = new FakeSource(
                List.of(snapshot(100), snapshot(101), snapshot(102)),
                List.of(change(0), change(1)));
        SrsCoordinator srsCoordinator = new SrsCoordinator(meta);
        CaptureRunUnit captureRunUnit = new CaptureRunUnit(fakeSource, srsCoordinator, meta, member);
        PipelineCaptureCoordinator coordinator =
                new StoreBackedPipelineCaptureCoordinator(store, captureRunUnit::start, srsCoordinator, snapshotBuffer);

        StoreBackedDagSource.SinkWriterBinder capturingSink =
                (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) CapturingSinkWriter::new;
        DagSource dagSource = new StoreBackedDagSource(store, capturingSink);
        LifecycleActuator actuator = new EngineLifecycleActuator(
                new Engine(member), dagSource, coordinator, new NestStateTeardown(member, store.keyedState()));

        actuator.start(PIPELINE);
        List<String> arrived;
        try {
            awaitSinkSize(5);
            arrived = List.copyOf(CapturingSinkWriter.collected());
        } finally {
            actuator.stop(PIPELINE);
        }

        // The load-bearing assertion: the three snapshot reads (op r) arrive first, in buffered order, then the
        // two cdc inserts (op i), in change order. Exact order proves all snapshot precedes all cdc AND that the
        // content is snapshot union cdc, all through the one transform-to-sink chain.
        assertThat(arrived).containsExactly("r|100", "r|101", "r|102", "i|0", "i|1");
    }

    private static Envelope snapshot(int id) {
        // A snapshot read: op r, no source position -- the value as it stands at snapshot time.
        return Envelope.read(id, TABLE, Map.of("id", (long) id), Map.of());
    }

    private static Envelope change(int id) {
        return Envelope.insert(id, TABLE, Map.of("id", (long) id), Map.of());
    }

    private void awaitSinkSize(int size) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (CapturingSinkWriter.collected().size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + size + " rows at the sink, got "
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

    /** A fake connector: a bounded snapshot batch and a fixed cdc stream driven synchronously when cdc starts. */
    private static final class FakeSource implements CapturePort {

        private final List<Envelope> snapshotRows;
        private final List<Envelope> changes;

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
            for (Envelope change : changes) {
                listener.onEvent(change);
            }
            return () -> { };
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

    /** A bounded snapshot batch over a fixed list of rows. */
    private static final class FakeBatch implements CaptureBatch {

        private final Iterator<Envelope> rows;

        FakeBatch(List<Envelope> rows) {
            this.rows = new ArrayList<>(rows).iterator();
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public Envelope next() {
            return rows.next();
        }

        @Override
        public void close() {
        }
    }

    /**
     * A sink writer that records each event as a stable {@code op|id} string into a JVM-static queue, so the
     * embedded run can assert the order the built DAG delivered. Shared with the test thread on one member.
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
                COLLECTED.add(record.op().symbol() + "|" + record.after().get("id"));
            }
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
